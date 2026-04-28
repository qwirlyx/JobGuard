from __future__ import annotations

import os
import re
from functools import wraps
from datetime import datetime, timezone, timedelta
from typing import Any
from uuid import uuid4

from flask import Flask, abort, render_template, redirect, url_for, flash, request, send_from_directory, jsonify
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from flask_wtf import FlaskForm
from flask_wtf.file import FileAllowed, FileField
from werkzeug.utils import secure_filename
from wtforms import StringField, PasswordField, SubmitField, SelectField, TextAreaField, IntegerField
from wtforms.validators import DataRequired, Length, EqualTo, Regexp, NumberRange, Optional
from werkzeug.security import generate_password_hash, check_password_hash
from config import Config
from storage import load_json_storage, save_json_storage

app = Flask(__name__)
app.config.from_object(Config)

os.makedirs(app.config['DATA_DIR'], exist_ok=True)
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

CHAT_UPLOAD_FOLDER = os.path.join(app.config['UPLOAD_FOLDER'], 'chat')
AVATAR_UPLOAD_FOLDER = os.path.join(app.config['UPLOAD_FOLDER'], 'avatars')
RESUME_UPLOAD_FOLDER = os.path.join(app.config['UPLOAD_FOLDER'], 'resumes')
ORDER_UPLOAD_FOLDER = os.path.join(app.config['UPLOAD_FOLDER'], 'orders')
ALLOWED_AVATAR_EXTENSIONS = {'jpg', 'jpeg', 'png', 'webp'}
MAX_AVATAR_SIZE = 2 * 1024 * 1024
SERVICE_FEE_PERCENT = 5
ORDER_FUND_FIXED_FEE = 300
ORDER_FUND_PERCENT_FEE = 3

STUDENT_STATUS_CHOICES = [
    ('studying', 'Обучаюсь сейчас'),
    ('graduate', 'Выпускник'),
    ('not_student', 'Не учусь'),
    ('other', 'Другое'),
]

EDUCATION_LEVEL_CHOICES = [
    ('bachelor', 'Бакалавриат'),
    ('specialist', 'Специалитет'),
    ('master', 'Магистратура'),
    ('postgraduate', 'Аспирантура'),
    ('college', 'СПО / колледж'),
    ('other', 'Другое'),
]

for folder in (CHAT_UPLOAD_FOLDER, AVATAR_UPLOAD_FOLDER, RESUME_UPLOAD_FOLDER, ORDER_UPLOAD_FOLDER):
    os.makedirs(folder, exist_ok=True)

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')


def _slugify(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r'\s+', '-', text)
    text = re.sub(r'[^a-z0-9а-яё\-]+', '', text)
    return text[:80] or 'file'


def _file_extension(filename: str) -> str:
    return filename.rsplit('.', 1)[1].lower() if '.' in filename else ''


def _is_allowed_avatar(filename: str) -> bool:
    return _file_extension(filename) in ALLOWED_AVATAR_EXTENSIONS


def _uploaded_file_size(file_storage) -> int:
    current_position = file_storage.stream.tell()
    file_storage.stream.seek(0, os.SEEK_END)
    size = file_storage.stream.tell()
    file_storage.stream.seek(current_position)
    return size


def _delete_avatar_file(filename: str) -> None:
    if not filename:
        return

    avatar_path = os.path.join(AVATAR_UPLOAD_FOLDER, filename)
    if os.path.isfile(avatar_path):
        os.remove(avatar_path)


def _save_avatar(file_storage, username: str) -> str:
    ext = _file_extension(file_storage.filename)
    safe_username = secure_filename(username) or 'user'
    filename = f'avatar-{safe_username}-{uuid4().hex}.{ext}'
    file_storage.save(os.path.join(AVATAR_UPLOAD_FOLDER, filename))
    return filename


def get_current_role() -> str | None:
    if not current_user.is_authenticated:
        return None
    u = users.get(current_user.id)
    return u.get('role') if u else None


def role_required(*roles: str):
    def decorator(view_func):
        @wraps(view_func)
        def wrapper(*args, **kwargs):
            role = get_current_role()
            if role not in roles:
                flash('Недостаточно прав для этого действия.', 'warning')
                return redirect(url_for('index'))
            return view_func(*args, **kwargs)
        return wrapper
    return decorator


def admin_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not current_user.is_authenticated or get_current_role() != 'admin':
            abort(403)
        return view_func(*args, **kwargs)
    return wrapper


def _find_username_by_login_or_email(value: str) -> str | None:
    lookup = (value or '').strip().lower()
    if not lookup:
        return None

    if lookup in users:
        return lookup

    for username, data in users.items():
        if username.lower() == lookup:
            return username

        email = (data.get('email') or '').strip().lower()
        if email and email == lookup:
            return username

    return None


def _email_exists(email: str) -> bool:
    lookup = (email or '').strip().lower()
    return any((data.get('email') or '').strip().lower() == lookup for data in users.values())


def _email_exists_for_another_user(email: str, current_username: str) -> bool:
    lookup = (email or '').strip().lower()
    for username, data in users.items():
        if username == current_username:
            continue
        user_email = (data.get('email') or '').strip().lower()
        if user_email and user_email == lookup:
            return True
    return False


def _money(value: int | float | str | None) -> str:
    try:
        amount = int(float(value or 0))
    except (TypeError, ValueError):
        amount = 0
    return f'{amount:,}'.replace(',', ' ')


def _get_reserve_amount(order: dict[str, Any]) -> int:
    try:
        price = int(order.get('price') or 0)
    except (TypeError, ValueError):
        price = 0
    return int(price * 0.5)


def _get_order_fee_amount(order: dict[str, Any]) -> int:
    try:
        price = int(order.get('price') or 0)
    except (TypeError, ValueError):
        price = 0
    return ORDER_FUND_FIXED_FEE + int(price * ORDER_FUND_PERCENT_FEE / 100)


def _get_order_total_to_pay(order: dict[str, Any]) -> int:
    try:
        price = int(order.get('price') or 0)
    except (TypeError, ValueError):
        price = 0
    return price + _get_order_fee_amount(order)


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ('%Y-%m-%d %H:%M', '%Y-%m-%dT%H:%M:%S'):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _order_deadline_at(order: dict[str, Any]) -> datetime | None:
    start = _parse_dt(order.get('accepted_at') or order.get('reserved_at') or order.get('created_at'))
    if not start:
        return None
    try:
        days = int(order.get('deadline_days') or 0)
    except (TypeError, ValueError):
        days = 0
    if days <= 0:
        return start
    return start + timedelta(days=days)


def _is_order_deadline_passed(order: dict[str, Any]) -> bool:
    deadline = _order_deadline_at(order)
    if not deadline:
        return False
    return datetime.now(timezone.utc) >= deadline


def _deadline_text(order: dict[str, Any]) -> str:
    deadline = _order_deadline_at(order)
    return deadline.strftime('%Y-%m-%d %H:%M') if deadline else 'не определён'


def _ensure_wallet(username: str) -> dict[str, Any]:
    user_data = users.get(username)
    if not user_data:
        abort(404)

    wallet = user_data.setdefault('wallet', {})
    wallet.setdefault('available', 0)
    wallet.setdefault('reserved', 0)
    wallet.setdefault('withdrawn', 0)
    wallet.setdefault('commission_paid', 0)
    wallet.setdefault('operations', [])
    wallet.setdefault('customer_spent', 0)
    wallet.setdefault('customer_frozen', 0)
    wallet.setdefault('customer_returned', 0)
    return wallet


def _wallet_operation(username: str, kind: str, amount: int, description: str, order_id: int | None = None, **extra: Any) -> None:
    wallet = _ensure_wallet(username)
    operation = {
        'id': len(wallet['operations']) + 1,
        'kind': kind,
        'amount': int(amount),
        'description': description,
        'order_id': order_id,
        'created_at': _now_iso(),
    }
    operation.update(extra)
    wallet['operations'].insert(0, operation)


def _ensure_order_finance(order: dict[str, Any]) -> dict[str, Any]:
    price = int(order.get('price') or 0)
    reserve_amount = _get_reserve_amount(order)

    order['reserve_amount'] = reserve_amount
    order.setdefault('funded_amount', 0)
    order.setdefault('payment_status', 'not_funded')
    order.setdefault('payment_history', [])
    order.setdefault('platform_fee_fixed', ORDER_FUND_FIXED_FEE)
    order.setdefault('platform_fee_percent', ORDER_FUND_PERCENT_FEE)
    order['platform_fee_amount'] = _get_order_fee_amount(order)
    order['fund_total_amount'] = _get_order_total_to_pay(order)

    # Совместимость со старой логикой deposit_status, если такие поля уже есть в JSON.
    old_deposit_status = order.get('deposit_status')
    if old_deposit_status and order.get('payment_status') == 'not_funded':
        if old_deposit_status == 'reserved':
            order['payment_status'] = 'reserved'
            order['funded_amount'] = price
            order.setdefault('reserved_amount', reserve_amount)
        elif old_deposit_status == 'paid':
            order['payment_status'] = 'paid'
            order['funded_amount'] = price
            order.setdefault('paid_amount', price)
        elif old_deposit_status == 'refunded':
            order['payment_status'] = 'refunded'

    return order


def _payment_status_text(status: str | None) -> str:
    labels = {
        'not_funded': 'Бюджет не пополнен',
        'funded': 'Бюджет пополнен',
        'reserved': '50% зарезервировано исполнителю',
        'paid': 'Оплачено исполнителю',
        'refunded': 'Возвращено заказчику',
        'disputed': 'Оплата в споре',
        'cancel_requested': 'Запрошена отмена',
    }
    return labels.get(status or 'not_funded', 'Бюджет не пополнен')


def _payment_status_class(status: str | None) -> str:
    classes = {
        'not_funded': 'bg-warning text-dark',
        'funded': 'bg-primary',
        'reserved': 'bg-info text-dark',
        'paid': 'bg-success',
        'refunded': 'bg-secondary',
        'disputed': 'bg-danger',
        'cancel_requested': 'bg-danger',
    }
    return classes.get(status or 'not_funded', 'bg-warning text-dark')



def _student_status_text(status: str | None) -> str:
    labels = dict(STUDENT_STATUS_CHOICES)
    labels['not_specified'] = 'Не указано'
    return labels.get(status or 'not_specified', 'Не указано')


def _education_level_text(level: str | None) -> str:
    labels = dict(EDUCATION_LEVEL_CHOICES)
    labels[''] = 'Не указано'
    return labels.get(level or '', 'Не указано')


def _empty_student_info() -> dict[str, Any]:
    return {
        'student_status': 'not_specified',
        'age': '',
        'institution': '',
        'faculty': '',
        'education_level': '',
        'course': '',
    }


def _ensure_student_info(username: str) -> dict[str, Any]:
    user_data = users.get(username)
    if not user_data:
        abort(404)

    info = user_data.setdefault('student_info', _empty_student_info())
    defaults = _empty_student_info()
    for key, value in defaults.items():
        info.setdefault(key, value)
    return info


def _is_priority_student(username: str) -> bool:
    user_data = users.get(username) or {}
    if user_data.get('role') != 'student':
        return False
    info = user_data.get('student_info') or {}
    return info.get('student_status') == 'studying'


def _student_education_summary(username: str) -> str:
    user_data = users.get(username) or {}
    info = user_data.get('student_info') or {}

    if user_data.get('role') != 'student':
        return ''

    status = info.get('student_status') or 'not_specified'
    if status == 'studying':
        parts = []
        if info.get('institution'):
            parts.append(info['institution'])
        if info.get('faculty'):
            parts.append(info['faculty'])
        if info.get('education_level'):
            parts.append(_education_level_text(info.get('education_level')))
        if info.get('course'):
            parts.append(f'{info["course"]} курс')
        return ' · '.join(parts) or 'Студент, данные обучения не заполнены'

    return _student_status_text(status)


def _ensure_notifications(username: str) -> list[dict[str, Any]]:
    user_data = users.get(username)
    if not user_data:
        return []
    return user_data.setdefault('notifications', [])


def _notify(username: str | None, title: str, text: str, url: str | None = None, kind: str = 'info') -> None:
    if not username or username not in users:
        return

    notifications = _ensure_notifications(username)
    next_id = max([int(n.get('id') or 0) for n in notifications] or [0]) + 1
    notifications.insert(0, {
        'id': next_id,
        'title': title,
        'text': text,
        'url': url,
        'kind': kind,
        'is_read': False,
        'created_at': _now_iso(),
    })
    del notifications[30:]


def _get_unread_notifications(username: str, limit: int = 5) -> list[dict[str, Any]]:
    notifications = _ensure_notifications(username)
    return [n for n in notifications if not n.get('is_read')][:limit]


def _ensure_default_admin() -> None:
    admin_username = 'admin'
    admin_data = users.get(admin_username)
    if admin_data:
        admin_data['role'] = 'admin'
        admin_data.setdefault('email', 'admin@jobguard.local')
        admin_data.setdefault('notifications', [])
        admin_data.setdefault('student_info', {})
        admin_data.setdefault('profile', {
            'display_name': 'Администратор JobGuard',
            'city': '',
            'skills': '',
            'about': 'Служебный аккаунт администратора платформы.',
            'github': '',
            'telegram': '',
            'portfolio': '',
            'resume_filename': '',
            'avatar_filename': '',
            'updated_at': _now_iso(),
            'projects': [],
        })
        admin_data.setdefault('wallet', {})
        return

    users[admin_username] = {
        'email': 'admin@jobguard.local',
        'password': generate_password_hash('admin123'),
        'role': 'admin',
        'student_info': {},
        'notifications': [],
        'profile': {
            'display_name': 'Администратор JobGuard',
            'city': '',
            'skills': '',
            'about': 'Служебный аккаунт администратора платформы.',
            'github': '',
            'telegram': '',
            'portfolio': '',
            'resume_filename': '',
            'avatar_filename': '',
            'updated_at': _now_iso(),
            'projects': [],
        },
        'wallet': {
            'available': 0,
            'reserved': 0,
            'withdrawn': 0,
            'commission_paid': 0,
            'operations': [],
            'customer_spent': 0,
            'customer_frozen': 0,
            'customer_returned': 0,
        },
    }


def _admin_usernames() -> list[str]:
    return [username for username, data in users.items() if data.get('role') == 'admin' and not data.get('is_blocked')]


def _notify_admins(title: str, text: str, url: str | None = None, kind: str = 'info') -> None:
    for admin_username in _admin_usernames():
        _notify(admin_username, title, text, url, kind)


def _support_category_text(category: str | None) -> str:
    labels = {
        'payment': 'Оплата и баланс',
        'order': 'Заказ или объявление',
        'student': 'Проблема с исполнителем',
        'customer': 'Проблема с заказчиком',
        'technical': 'Техническая проблема',
        'dispute': 'Спорная ситуация',
        'other': 'Другое',
    }
    return labels.get(category or 'other', 'Другое')


def _support_status_text(status: str | None) -> str:
    labels = {
        'new': 'Новое',
        'in_progress': 'В работе',
        'closed': 'Закрыто',
    }
    return labels.get(status or 'new', 'Новое')


def _support_status_class(status: str | None) -> str:
    classes = {
        'new': 'bg-danger',
        'in_progress': 'bg-warning text-dark',
        'closed': 'bg-success',
    }
    return classes.get(status or 'new', 'bg-danger')


def _get_ticket(ticket_id: int) -> dict[str, Any]:
    for ticket in support_tickets:
        if int(ticket.get('id') or 0) == int(ticket_id):
            return ticket
    abort(404)


def _get_admin_stats() -> dict[str, Any]:
    all_orders = [_ensure_order_finance(o) for o in orders]
    funded_orders = [o for o in all_orders if o.get('payment_status') in {'funded', 'reserved', 'paid', 'refunded', 'disputed'}]
    return {
        'users_total': len(users),
        'students_total': len([u for u in users.values() if u.get('role') == 'student']),
        'customers_total': len([u for u in users.values() if u.get('role') == 'customer']),
        'admins_total': len([u for u in users.values() if u.get('role') == 'admin']),
        'blocked_total': len([u for u in users.values() if u.get('is_blocked')]),
        'orders_total': len(orders),
        'active_orders': len([o for o in orders if o.get('status') in {'open', 'has_responses', 'in_progress', 'review'}]),
        'dispute_orders': len([o for o in orders if o.get('status') == 'dispute']),
        'done_orders': len([o for o in orders if o.get('status') == 'done']),
        'support_new': len([t for t in support_tickets if t.get('status') == 'new']),
        'support_open': len([t for t in support_tickets if t.get('status') in {'new', 'in_progress'}]),
        'support_total': len(support_tickets),
        'funded_total': sum(int(o.get('fund_total_amount') or 0) for o in funded_orders),
        'platform_fee_total': sum(int(o.get('platform_fee_amount') or 0) for o in funded_orders),
        'reserved_total': sum(int(o.get('reserved_amount') or o.get('reserve_amount') or 0) for o in all_orders if o.get('payment_status') in {'reserved', 'disputed'}),
        'paid_total': sum(int(o.get('paid_amount') or 0) for o in all_orders if o.get('payment_status') == 'paid'),
        'refunded_total': sum(int(o.get('funded_amount') or 0) for o in all_orders if o.get('payment_status') == 'refunded'),
    }


users: dict[str, dict[str, Any]] = {}
orders: list[dict[str, Any]] = [
    {
        'id': 1,
        'title': 'Инфографика для Wildberries (5 карточек)',
        'price': 15000,
        'deadline_days': 3,
        'description': 'Нужно сделать стильные карточки товаров для WB. Требуется опыт с инфографикой и Canva/Figma.',
        'tags': ['Дизайн', 'Wildberries', 'Срочно'],
        'created_at': _now_iso(),
        'owner': 'demo_customer',
        'status': 'open',
        'executor': None,
    },
    {
        'id': 2,
        'title': 'Telegram-бот для магазина',
        'price': 25000,
        'deadline_days': 10,
        'description': 'Нужен бот с каталогом товаров, корзиной и оплатой через ЮKassa.',
        'tags': ['Telegram-бот', 'Python', 'aiogram'],
        'created_at': _now_iso(),
        'owner': 'demo_customer',
        'status': 'open',
        'executor': None,
    },
    {
        'id': 3,
        'title': 'Мобильное приложение на Flutter',
        'price': 80000,
        'deadline_days': 25,
        'description': 'Нужен кросс-платформенный магазин одежды с авторизацией и push-уведомлениями.',
        'tags': ['Мобильное приложение', 'Flutter', 'Средний'],
        'created_at': _now_iso(),
        'owner': 'demo_customer',
        'status': 'open',
        'executor': None,
    },
]
applications: list[dict[str, Any]] = []
conversations: list[dict[str, Any]] = []
reviews: list[dict[str, Any]] = []
support_tickets: list[dict[str, Any]] = []

_storage = load_json_storage(
    app.config['DATA_FILE'],
    {
        'users': users,
        'orders': orders,
        'applications': applications,
        'conversations': conversations,
        'reviews': reviews,
        'support_tickets': support_tickets,
    },
)

users = _storage['users']
orders = _storage['orders']
applications = _storage['applications']
conversations = _storage['conversations']
reviews = _storage['reviews']
support_tickets = _storage.setdefault('support_tickets', [])

for _order in orders:
    _ensure_order_finance(_order)
for _username in list(users.keys()):
    _ensure_wallet(_username)
    _ensure_notifications(_username)
    if (users.get(_username) or {}).get('role') == 'student':
        _ensure_student_info(_username)


def save_all() -> None:
    for order in orders:
        _ensure_order_finance(order)
    for username in list(users.keys()):
        _ensure_wallet(username)
        _ensure_notifications(username)
        if (users.get(username) or {}).get('role') == 'student':
            _ensure_student_info(username)

    save_json_storage(
        app.config['DATA_FILE'],
        {
            'users': users,
            'orders': orders,
            'applications': applications,
            'conversations': conversations,
            'reviews': reviews,
            'support_tickets': support_tickets,
        },
    )


_ensure_default_admin()
save_all()


class User(UserMixin):
    def __init__(self, username):
        self.id = username


@login_manager.user_loader
def load_user(username):
    if username in users:
        return User(username)
    return None


@app.before_request
def restrict_admin_to_admin_panel():
    if not current_user.is_authenticated or get_current_role() != 'admin':
        return None

    endpoint = request.endpoint or ''
    allowed = (
        endpoint == 'static'
        or endpoint == 'logout'
        or endpoint.startswith('admin_')
    )

    if allowed:
        return None

    return redirect(url_for('admin_dashboard'))


class RegisterForm(FlaskForm):
    username = StringField(
        'Логин',
        validators=[
            DataRequired(message='Введите логин.'),
            Length(min=4, max=20, message='Логин должен быть от 4 до 20 символов.'),
            Regexp(r'^[A-Za-z0-9_]+$', message='Логин может содержать только латинские буквы, цифры и подчёркивание.'),
        ],
    )
    email = StringField(
        'Email',
        validators=[
            DataRequired(message='Введите email.'),
            Length(max=120, message='Email слишком длинный.'),
            Regexp(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', message='Введите корректный email.'),
        ],
    )
    password = PasswordField(
        'Пароль',
        validators=[DataRequired(message='Введите пароль.'), Length(min=6, max=128, message='Пароль должен быть минимум 6 символов.')],
    )
    confirm_password = PasswordField(
        'Повторите пароль',
        validators=[DataRequired(message='Повторите пароль.'), EqualTo('password', message='Пароли не совпадают.')],
    )
    role = SelectField('Роль', choices=[('student', 'Студент'), ('customer', 'Заказчик')], validators=[DataRequired(message='Выберите роль.')])
    student_status = SelectField('Статус обучения', choices=STUDENT_STATUS_CHOICES, validators=[Optional()])
    age = IntegerField('Возраст', validators=[Optional(), NumberRange(min=14, max=80, message='Возраст должен быть от 14 до 80 лет.')])
    institution = StringField('Институт / университет', validators=[Optional(), Length(max=160, message='Название учебного заведения слишком длинное.')])
    faculty = StringField('Факультет / направление', validators=[Optional(), Length(max=160, message='Название факультета слишком длинное.')])
    education_level = SelectField('Уровень образования', choices=EDUCATION_LEVEL_CHOICES, validators=[Optional()])
    course = IntegerField('Курс', validators=[Optional(), NumberRange(min=1, max=6, message='Курс должен быть от 1 до 6.')])
    submit = SubmitField('Зарегистрироваться')


class LoginForm(FlaskForm):
    username = StringField('Логин или email', validators=[DataRequired(message='Введите логин или email.')])
    password = PasswordField('Пароль', validators=[DataRequired(message='Введите пароль.')])
    submit = SubmitField('Войти')


class OrderCreateForm(FlaskForm):
    title = StringField('Название', validators=[DataRequired(message='Введите название заказа.'), Length(min=6, max=120, message='Название должно быть от 6 до 120 символов.')])
    description = TextAreaField('Описание', validators=[DataRequired(message='Введите описание заказа.'), Length(min=20, max=3000, message='Описание должно быть от 20 до 3000 символов.')])
    price = IntegerField('Бюджет (₽)', validators=[DataRequired(message='Введите бюджет.'), NumberRange(min=500, max=10000000, message='Бюджет должен быть от 500 до 10 000 000 ₽.')])
    deadline_days = IntegerField('Срок (дней)', validators=[DataRequired(message='Введите срок выполнения.'), NumberRange(min=1, max=365, message='Срок должен быть от 1 до 365 дней.')])
    tags = StringField('Теги (через запятую)', validators=[DataRequired(message='Введите хотя бы один тег.'), Length(min=2, max=200, message='Теги должны быть от 2 до 200 символов.')])
    submit = SubmitField('Опубликовать заказ')


class AccountSettingsForm(FlaskForm):
    display_name = StringField('Отображаемое имя', validators=[DataRequired(message='Введите отображаемое имя.'), Length(min=2, max=60, message='Имя должно быть от 2 до 60 символов.')])
    email = StringField('Email', validators=[DataRequired(message='Введите email.'), Length(max=120, message='Email слишком длинный.'), Regexp(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', message='Введите корректный email.')])
    city = StringField('Город', validators=[Optional(), Length(max=80, message='Название города слишком длинное.')])
    telegram = StringField('Telegram', validators=[Optional(), Length(max=80, message='Telegram слишком длинный.')])
    github = StringField('GitHub', validators=[Optional(), Length(max=200, message='Ссылка на GitHub слишком длинная.')])
    portfolio = StringField('Портфолио', validators=[Optional(), Length(max=200, message='Ссылка на портфолио слишком длинная.')])
    avatar = FileField('Аватарка', validators=[FileAllowed(['jpg', 'jpeg', 'png', 'webp'], 'Только JPG, JPEG, PNG или WEBP.')])
    submit = SubmitField('Сохранить настройки')


class PasswordChangeForm(FlaskForm):
    current_password = PasswordField('Текущий пароль', validators=[DataRequired(message='Введите текущий пароль.')])
    new_password = PasswordField('Новый пароль', validators=[DataRequired(message='Введите новый пароль.'), Length(min=6, max=128, message='Пароль должен быть минимум 6 символов.')])
    confirm_password = PasswordField('Повторите новый пароль', validators=[DataRequired(message='Повторите новый пароль.'), EqualTo('new_password', message='Пароли не совпадают.')])
    submit = SubmitField('Сменить пароль')


class ProfileForm(FlaskForm):
    skills = StringField('Навыки (через запятую)', validators=[Length(max=250)])
    about = TextAreaField('О себе', validators=[Length(max=1200)])
    resume = FileField('Резюме (PDF/DOC/DOCX)', validators=[FileAllowed(['pdf', 'doc', 'docx'], 'Только PDF/DOC/DOCX')])
    submit = SubmitField('Сохранить профиль')


class ReviewForm(FlaskForm):
    rating = SelectField('Оценка', choices=[(str(i), f'{i} звезд') for i in range(1, 6)], validators=[DataRequired()])
    text = TextAreaField('Отзыв', validators=[DataRequired(), Length(min=10, max=1000)])
    submit = SubmitField('Оставить отзыв')


def _get_profile(username: str) -> dict[str, Any]:
    u = users.get(username)
    if not u:
        abort(404)
    return u.setdefault(
        'profile',
        {
            'display_name': username,
            'city': '',
            'skills': '',
            'about': '',
            'github': '',
            'telegram': '',
            'portfolio': '',
            'resume_filename': '',
            'avatar_filename': '',
            'updated_at': _now_iso(),
            'projects': [],
        },
    )


def _user_display_name(username: str | None) -> str:
    if not username:
        return ''
    if username == 'system':
        return 'Система'
    u = users.get(username) or {}
    profile = u.get('profile') or {}
    name = (profile.get('display_name') or '').strip()
    return name or str(username)


def _review_target_username(review: dict[str, Any]) -> str:
    return review.get('target_username') or review.get('student_username') or ''


def _review_target_role(review: dict[str, Any]) -> str:
    return review.get('target_role') or 'student'


def _get_user_rating(username: str) -> dict[str, Any]:
    user_reviews = [r for r in reviews if _review_target_username(r) == username]
    user_reviews = sorted(user_reviews, key=lambda r: r.get('created_at', ''), reverse=True)

    if not user_reviews:
        return {'avg': 0, 'count': 0, 'reviews': []}

    avg_rating = sum(int(r.get('rating') or 0) for r in user_reviews) / len(user_reviews)
    return {'avg': round(avg_rating, 1), 'count': len(user_reviews), 'reviews': user_reviews}


def _has_review(order_id: int, reviewer_username: str, target_username: str | None = None) -> bool:
    for review in reviews:
        if int(review.get('order_id') or 0) != int(order_id):
            continue
        if review.get('reviewer_username') != reviewer_username:
            continue
        if target_username and _review_target_username(review) != target_username:
            continue
        return True
    return False


def _get_order_title(order_id: int) -> str:
    for order in orders:
        if int(order.get('id') or 0) == int(order_id):
            return order.get('title') or f'Заказ #{order_id}'
    return f'Заказ #{order_id}'


def _status_label(status: str | None) -> str:
    labels = {
        'open': 'Открыт',
        'has_responses': 'Есть отклики',
        'in_progress': 'В работе',
        'review': 'На проверке',
        'done': 'Завершён',
        'cancelled': 'Отменён',
        'cancelled_by_customer': 'Отменён заказчиком',
        'cancelled_by_student': 'Исполнитель отказался',
        'cancel_requested': 'Запрошена отмена',
        'dispute': 'Спор',
    }
    return labels.get(status or 'open', 'Открыт')


def _status_class(status: str | None) -> str:
    classes = {
        'open': 'bg-success',
        'has_responses': 'bg-warning text-dark',
        'in_progress': 'bg-info text-dark',
        'review': 'bg-primary',
        'done': 'bg-secondary',
        'cancelled': 'bg-danger',
        'cancelled_by_customer': 'bg-danger',
        'cancelled_by_student': 'bg-danger',
        'cancel_requested': 'bg-danger',
        'dispute': 'bg-danger',
    }
    return classes.get(status or 'open', 'bg-success')


def _get_platform_stats() -> dict[str, Any]:
    active_orders = len([o for o in orders if o['status'] in ('open', 'has_responses', 'in_progress', 'review')])
    students_count = len([u for u in users.values() if u.get('role') == 'student'])
    completed_orders = len([o for o in orders if o['status'] == 'done'])
    total_finished = completed_orders + len([o for o in orders if o['status'] in ('in_progress', 'review')])
    success_rate = round((completed_orders / total_finished * 100) if total_finished > 0 else 0)
    avg_rating = round(sum(r['rating'] for r in reviews) / len(reviews), 1) if reviews else 0.0
    return {
        'active_orders': active_orders,
        'students_count': students_count,
        'success_rate': success_rate,
        'avg_rating': avg_rating,
        'completed_orders': completed_orders,
        'total_reviews': len(reviews),
    }


@app.context_processor
def inject_globals():
    def is_active(*endpoints: str) -> bool:
        return request.endpoint in endpoints

    def get_avatar(username: str) -> str | None:
        u = users.get(username)
        if u:
            profile = u.get('profile', {})
            return profile.get('avatar_filename')
        return None

    role = get_current_role()
    unread_notifications = _get_unread_notifications(current_user.id) if current_user.is_authenticated else []
    return {
        'is_active': is_active,
        'current_role': role,
        'get_avatar': get_avatar,
        'platform_stats': _get_platform_stats(),
        'money': _money,
        'payment_status_text': _payment_status_text,
        'payment_status_class': _payment_status_class,
        'service_fee_percent': SERVICE_FEE_PERCENT,
        'order_fund_fixed_fee': ORDER_FUND_FIXED_FEE,
        'order_fund_percent_fee': ORDER_FUND_PERCENT_FEE,
        'order_fee_amount': _get_order_fee_amount,
        'order_total_to_pay': _get_order_total_to_pay,
        'order_deadline_text': _deadline_text,
        'order_deadline_passed': _is_order_deadline_passed,
        'status_label': _status_label,
        'status_class': _status_class,
        'unread_notifications': unread_notifications,
        'student_status_text': _student_status_text,
        'education_level_text': _education_level_text,
        'student_education_summary': _student_education_summary,
        'is_priority_student': _is_priority_student,
        'user_display_name': _user_display_name,
        'support_category_text': _support_category_text,
        'support_status_text': _support_status_text,
        'support_status_class': _support_status_class,
    }


def _get_order(order_id: int) -> dict[str, Any]:
    for o in orders:
        if o['id'] == order_id:
            return _ensure_order_finance(o)
    abort(404)


def _get_conversation(conv_id: int) -> dict[str, Any]:
    for c in conversations:
        if c['id'] == conv_id:
            return c
    abort(404)


def _create_conversation(order: dict[str, Any], student_username: str) -> dict[str, Any]:
    customer_username = order.get('owner')
    conv_id = len(conversations) + 1
    conv = {
        'id': conv_id,
        'order_id': order['id'],
        'student': student_username,
        'customer': customer_username,
        'created_at': _now_iso(),
        'messages': [],
    }
    conversations.append(conv)
    return conv


def _message_payload(message: dict[str, Any], current_username: str) -> dict[str, Any]:
    sender = message.get('sender') or ''
    kind = message.get('kind') or 'text'
    return {
        'sender': sender,
        'sender_name': _user_display_name(sender),
        'is_me': sender == current_username,
        'kind': kind,
        'text': message.get('text') or '',
        'created_at': message.get('created_at') or '',
        'file_name': message.get('file_name') or '',
        'file_stored': message.get('file_stored') or '',
        'file_url': url_for('chat_file', filename=message.get('file_stored')) if kind == 'file' and message.get('file_stored') else '',
    }


def _conversation_payload(conv: dict[str, Any]) -> list[dict[str, Any]]:
    return [_message_payload(m, current_user.id) for m in conv.get('messages', [])]


def _notify_message_counterpart(conv: dict[str, Any], text: str = 'Новое сообщение') -> None:
    recipient = conv.get('customer') if current_user.id == conv.get('student') else conv.get('student')
    if recipient and recipient != current_user.id:
        _notify(
            recipient,
            'Новое сообщение',
            text,
            url_for('conversation', conv_id=conv.get('id')),
            'info',
        )


def _remove_executor_reserve(order: dict[str, Any]) -> None:
    executor = order.get('executor')
    if not executor:
        return
    reserve_amount = int(order.get('reserved_amount') or order.get('reserve_amount') or 0)
    wallet = _ensure_wallet(executor)
    wallet['reserved'] = max(0, int(wallet.get('reserved', 0)) - reserve_amount)
    if reserve_amount:
        _wallet_operation(
            executor,
            'reserve_cancelled',
            reserve_amount,
            f'Резерв снят по заказу #{order["id"]}',
            order.get('id'),
        )


def _refund_order_budget_to_customer(order: dict[str, Any], reason: str) -> None:
    customer = order.get('owner')
    if not customer:
        return
    amount = int(order.get('funded_amount') or order.get('price') or 0)
    if amount <= 0:
        return
    wallet = _ensure_wallet(customer)
    wallet['available'] += amount
    wallet['customer_returned'] += amount
    _wallet_operation(customer, 'refund', amount, reason, order.get('id'))


@app.route('/')
def index():
    return render_template('index.html', orders=orders[:3])


@app.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('index'))

    form = RegisterForm()
    if form.validate_on_submit():
        username = form.username.data.strip().lower()
        email = form.email.data.strip().lower()

        if username in users:
            flash('Пользователь с таким логином уже существует.', 'danger')
            return redirect(url_for('register'))

        if _email_exists(email):
            flash('Пользователь с таким email уже существует.', 'danger')
            return redirect(url_for('register'))

        student_info = _empty_student_info()
        if form.role.data == 'student':
            student_status = form.student_status.data or ''
            if student_status not in {choice[0] for choice in STUDENT_STATUS_CHOICES}:
                flash('Укажите статус обучения.', 'warning')
                return render_template('register.html', form=form)

            if not form.age.data:
                flash('Для студенческого профиля укажите возраст.', 'warning')
                return render_template('register.html', form=form)

            if student_status == 'studying':
                if not (form.institution.data or '').strip():
                    flash('Для обучающегося студента укажите институт или университет.', 'warning')
                    return render_template('register.html', form=form)
                if not (form.faculty.data or '').strip():
                    flash('Для обучающегося студента укажите факультет или направление.', 'warning')
                    return render_template('register.html', form=form)
                if not form.course.data:
                    flash('Для обучающегося студента укажите курс.', 'warning')
                    return render_template('register.html', form=form)

            student_info = {
                'student_status': student_status,
                'age': int(form.age.data or 0),
                'institution': (form.institution.data or '').strip(),
                'faculty': (form.faculty.data or '').strip(),
                'education_level': form.education_level.data or '',
                'course': int(form.course.data or 0) if form.course.data else '',
            }

        users[username] = {
            'email': email,
            'password': generate_password_hash(form.password.data),
            'role': form.role.data,
            'student_info': student_info if form.role.data == 'student' else {},
            'notifications': [],
            'profile': {
                'display_name': username,
                'city': '',
                'skills': '',
                'about': '',
                'github': '',
                'telegram': '',
                'portfolio': '',
                'resume_filename': '',
                'avatar_filename': '',
                'updated_at': _now_iso(),
                'projects': [],
            },
            'wallet': {
                'available': 0,
                'reserved': 0,
                'withdrawn': 0,
                'commission_paid': 0,
                'operations': [],
            },
        }

        save_all()
        login_user(User(username))
        flash('Регистрация успешна. Вы вошли в аккаунт.', 'success')
        return redirect(url_for('index'))

    return render_template('register.html', form=form)


@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        if get_current_role() == 'admin':
            return redirect(url_for('admin_dashboard'))
        return redirect(url_for('index'))

    form = LoginForm()
    if form.validate_on_submit():
        username = _find_username_by_login_or_email(form.username.data)
        if username and username in users and check_password_hash(users[username]['password'], form.password.data):
            if users[username].get('is_blocked'):
                flash('Аккаунт заблокирован администратором.', 'danger')
                return redirect(url_for('login'))
            login_user(User(username))
            flash('Вы вошли!', 'success')
            if users[username].get('role') == 'admin':
                return redirect(url_for('admin_dashboard'))
            return redirect(url_for('index'))
        flash('Неверный логин, email или пароль.', 'danger')

    return render_template('login.html', form=form)


@app.route('/orders')
def orders_list():
    q = (request.args.get('q') or '').strip()
    tag = (request.args.get('tag') or '').strip()
    filtered = [_ensure_order_finance(o) for o in orders]
    if q:
        ql = q.lower()
        filtered = [
            o for o in filtered
            if ql in o['title'].lower() or ql in o['description'].lower() or any(ql in t.lower() for t in o['tags'])
        ]
    if tag:
        tl = tag.lower()
        filtered = [o for o in filtered if any(tl == t.lower() for t in o['tags'])]
    return render_template('orders.html', orders=filtered, q=q, tag=tag)


@app.route('/orders/<int:order_id>')
def order_detail(order_id: int):
    order = _get_order(order_id)
    user_applied = False
    user_application = None
    if current_user.is_authenticated:
        user_application = next((a for a in applications if a['order_id'] == order_id and a['username'] == current_user.id), None)
        user_applied = user_application is not None
    return render_template('order_detail.html', order=order, user_applied=user_applied, user_application=user_application)


@app.route('/orders/<int:order_id>/apply', methods=['POST'])
@login_required
@role_required('student')
def order_apply(order_id: int):
    order = _get_order(order_id)
    if order.get('status') not in {'open', 'has_responses'}:
        flash('Этот заказ больше не принимает отклики.', 'warning')
        return redirect(url_for('order_detail', order_id=order_id))
    if any(a['order_id'] == order_id and a['username'] == current_user.id for a in applications):
        flash('Вы уже откликались на этот заказ.', 'info')
        return redirect(url_for('order_detail', order_id=order_id))

    message = (request.form.get('message') or '').strip()
    if len(message) > 1000:
        flash('Сообщение отклика слишком длинное. Максимум 1000 символов.', 'warning')
        return redirect(url_for('order_detail', order_id=order_id))

    conv = _create_conversation(order, current_user.id)
    if message:
        conv['messages'].append({
            'sender': current_user.id,
            'text': message,
            'created_at': _now_iso(),
            'kind': 'text',
        })
    applications.append({
        'id': len(applications) + 1,
        'order_id': order['id'],
        'username': current_user.id,
        'message': message,
        'created_at': _now_iso(),
        'status': 'pending',
        'conversation_id': conv['id'],
    })

    if order.get('status') == 'open':
        order['status'] = 'has_responses'

    _notify(
        order.get('owner'),
        'Новый отклик',
        f'{_user_display_name(current_user.id)} откликнулся на заказ #{order["id"]}',
        url_for('my_orders'),
        'info',
    )

    save_all()
    flash('Отклик отправлен. Заказчик увидит его в разделе "Мои заказы".', 'success')
    return redirect(url_for('responses'))


@app.route('/orders/new', methods=['GET', 'POST'])
@login_required
@role_required('customer')
def order_new():
    form = OrderCreateForm()
    if form.validate_on_submit():
        new_id = max([o['id'] for o in orders] or [0]) + 1
        tags = [t.strip() for t in (form.tags.data or '').split(',') if t.strip()]
        price = int(form.price.data)
        orders.insert(0, {
            'id': new_id,
            'title': form.title.data.strip(),
            'price': price,
            'reserve_amount': int(price * 0.5),
            'funded_amount': 0,
            'payment_status': 'not_funded',
            'payment_history': [],
            'deadline_days': int(form.deadline_days.data),
            'description': form.description.data.strip(),
            'tags': tags[:8],
            'created_at': _now_iso(),
            'updated_at': _now_iso(),
            'owner': current_user.id,
            'status': 'open',
            'executor': None,
        })
        save_all()
        flash('Заказ опубликован. Теперь можно пополнить бюджет объявления.', 'success')
        return redirect(url_for('order_detail', order_id=new_id))
    return render_template('order_new.html', form=form)


@app.route('/orders/<int:order_id>/edit', methods=['GET', 'POST'])
@login_required
@role_required('customer')
def order_edit(order_id: int):
    order = _get_order(order_id)
    if order.get('owner') != current_user.id:
        abort(403)
    if order.get('status') not in {'open', 'has_responses'}:
        flash('Редактировать можно только открытый заказ или заказ с откликами.', 'warning')
        return redirect(url_for('order_detail', order_id=order_id))
    if order.get('payment_status') not in {'not_funded', 'refunded'}:
        flash('Нельзя редактировать бюджет после пополнения объявления.', 'warning')
        return redirect(url_for('order_detail', order_id=order_id))

    form = OrderCreateForm(
        title=order.get('title', ''),
        description=order.get('description', ''),
        price=order.get('price', 0),
        deadline_days=order.get('deadline_days', 1),
        tags=', '.join(order.get('tags', [])),
    )
    form.submit.label.text = 'Сохранить изменения'

    if form.validate_on_submit():
        tags = [t.strip() for t in (form.tags.data or '').split(',') if t.strip()]
        order['title'] = form.title.data.strip()
        order['description'] = form.description.data.strip()
        order['price'] = int(form.price.data)
        order['deadline_days'] = int(form.deadline_days.data)
        order['tags'] = tags[:8]
        order['updated_at'] = _now_iso()
        _ensure_order_finance(order)
        save_all()
        flash('Заказ обновлён.', 'success')
        return redirect(url_for('order_detail', order_id=order_id))
    return render_template('order_edit.html', form=form, order=order)


@app.route('/orders/<int:order_id>/fund', methods=['GET', 'POST'])
@login_required
@role_required('customer')
def order_fund(order_id: int):
    order = _get_order(order_id)
    if order.get('owner') != current_user.id:
        abort(403)
    if order.get('payment_status') in {'funded', 'reserved', 'paid'}:
        flash('Бюджет этого объявления уже пополнен.', 'info')
        return redirect(url_for('order_detail', order_id=order_id))
    if order.get('status') not in {'open', 'has_responses'}:
        flash('Пополнить бюджет можно только до выбора исполнителя.', 'warning')
        return redirect(url_for('order_detail', order_id=order_id))

    fee_amount = _get_order_fee_amount(order)
    total_to_pay = _get_order_total_to_pay(order)

    if request.method == 'POST':
        order['funded_amount'] = int(order.get('price') or 0)
        order['platform_fee_fixed'] = ORDER_FUND_FIXED_FEE
        order['platform_fee_percent'] = ORDER_FUND_PERCENT_FEE
        order['platform_fee_amount'] = fee_amount
        order['fund_total_amount'] = total_to_pay
        order['payment_status'] = 'funded'
        order['funded_at'] = _now_iso()
        order['payment_history'].insert(0, {
            'kind': 'fund',
            'amount': order['funded_amount'],
            'fee_amount': fee_amount,
            'total_amount': total_to_pay,
            'description': 'Демонстрационное пополнение бюджета объявления с комиссией площадки',
            'created_at': _now_iso(),
        })

        customer_wallet = _ensure_wallet(current_user.id)
        customer_wallet['customer_spent'] += total_to_pay
        customer_wallet['customer_frozen'] += order['funded_amount']
        _wallet_operation(
            current_user.id,
            'order_fund',
            total_to_pay,
            f'Пополнение объявления #{order["id"]}: бюджет {_money(order["funded_amount"])} ₽ + комиссия {_money(fee_amount)} ₽',
            order['id'],
            budget_amount=order['funded_amount'],
            fee_amount=fee_amount,
        )

        save_all()
        flash(f'Бюджет объявления пополнен. К оплате учтено {_money(total_to_pay)} ₽, включая комиссию площадки {_money(fee_amount)} ₽.', 'success')
        return redirect(url_for('order_detail', order_id=order_id))

    return render_template('order_fund.html', order=order, fee_amount=fee_amount, total_to_pay=total_to_pay)


@app.route('/responses')
@login_required
@role_required('student')
def responses():
    mine = [a for a in applications if a['username'] == current_user.id]
    by_id = {o['id']: _ensure_order_finance(o) for o in orders}
    enriched = [{**a, 'order': by_id.get(a['order_id'])} for a in mine]
    reviewed_order_ids = [
        int(r.get('order_id') or 0)
        for r in reviews
        if r.get('reviewer_username') == current_user.id
    ]
    return render_template('responses.html', responses=enriched, reviewed_order_ids=reviewed_order_ids)


@app.route('/my-orders')
@login_required
@role_required('customer')
def my_orders():
    mine = [_ensure_order_finance(o) for o in orders if o.get('owner') == current_user.id]
    apps_by_order: dict[int, list[dict[str, Any]]] = {}
    for a in applications:
        apps_by_order.setdefault(a['order_id'], []).append(a)
    reviewed_order_ids = [
        int(r.get('order_id') or 0)
        for r in reviews
        if r.get('reviewer_username') == current_user.id
    ]
    return render_template(
        'my_orders.html',
        orders=mine,
        apps_by_order=apps_by_order,
        reviewed_order_ids=reviewed_order_ids,
    )


@app.route('/messages')
@login_required
def messages():
    username = current_user.id
    my_convs = [c for c in conversations if c['student'] == username or c['customer'] == username]
    by_id = {o['id']: _ensure_order_finance(o) for o in orders}
    items = []
    for c in my_convs:
        order = by_id.get(c['order_id'])
        last_msg = c['messages'][-1] if c['messages'] else None
        counterpart = c['customer'] if username == c['student'] else c['student']
        items.append({
            'conversation': c,
            'order': order,
            'counterpart': counterpart,
            'counterpart_name': _user_display_name(counterpart),
            'last_message': last_msg,
            'last_sender_name': _user_display_name((last_msg or {}).get('sender')),
        })
    items.sort(key=lambda item: (item['last_message'] or {}).get('created_at', item['conversation'].get('created_at', '')), reverse=True)
    return render_template('messages.html', threads=items)


@app.route('/messages/<int:conv_id>', methods=['GET', 'POST'])
@login_required
def conversation(conv_id: int):
    conv = _get_conversation(conv_id)
    if current_user.id not in (conv['student'], conv['customer']):
        abort(403)
    order = _get_order(conv['order_id'])

    if current_user.id == conv.get('customer'):
        app_item = next((a for a in applications if a.get('conversation_id') == conv_id), None)
        if app_item and not app_item.get('customer_opened_chat'):
            app_item['customer_opened_chat'] = True
            app_item['customer_opened_chat_at'] = _now_iso()
            conv['customer_opened_chat'] = True
            save_all()

    if request.method == 'POST':
        text = (request.form.get('text') or '').strip()
        file = request.files.get('file')
        added_message = False
        if text:
            conv['messages'].append({'sender': current_user.id, 'text': text, 'created_at': _now_iso(), 'kind': 'text'})
            added_message = True
        if file and file.filename:
            original = secure_filename(file.filename)
            ext = (original.rsplit('.', 1)[-1].lower() if '.' in original else '')
            if ext in {'pdf', 'doc', 'docx'}:
                stored = f'{conv_id}-{int(datetime.now(timezone.utc).timestamp())}-{original}'
                file.save(os.path.join(CHAT_UPLOAD_FOLDER, stored))
                conv['messages'].append({'sender': current_user.id, 'text': '', 'created_at': _now_iso(), 'kind': 'file', 'file_name': original, 'file_stored': stored})
                added_message = True
        if added_message:
            _notify_message_counterpart(conv, f'{_user_display_name(current_user.id)}: {text[:80] if text else "файл"}')
        save_all()
        return redirect(url_for('conversation', conv_id=conv_id))
    return render_template('conversation.html', conv=conv, order=order, messages_payload=_conversation_payload(conv))


@app.route('/messages/<int:conv_id>/api')
@login_required
def conversation_api(conv_id: int):
    conv = _get_conversation(conv_id)
    if current_user.id not in (conv['student'], conv['customer']):
        abort(403)
    return jsonify({'messages': _conversation_payload(conv), 'count': len(conv.get('messages', []))})


@app.route('/messages/<int:conv_id>/send', methods=['POST'])
@login_required
def conversation_send(conv_id: int):
    conv = _get_conversation(conv_id)
    if current_user.id not in (conv['student'], conv['customer']):
        abort(403)

    text = (request.form.get('text') or '').strip()
    file = request.files.get('file')
    if not text and not (file and file.filename):
        return jsonify({'ok': False, 'error': 'empty'}), 400

    if text:
        conv['messages'].append({'sender': current_user.id, 'text': text, 'created_at': _now_iso(), 'kind': 'text'})

    if file and file.filename:
        original = secure_filename(file.filename)
        ext = (original.rsplit('.', 1)[-1].lower() if '.' in original else '')
        if ext not in {'pdf', 'doc', 'docx'}:
            return jsonify({'ok': False, 'error': 'bad_file'}), 400
        stored = f'{conv_id}-{int(datetime.now(timezone.utc).timestamp())}-{original}'
        file.save(os.path.join(CHAT_UPLOAD_FOLDER, stored))
        conv['messages'].append({'sender': current_user.id, 'text': '', 'created_at': _now_iso(), 'kind': 'file', 'file_name': original, 'file_stored': stored})

    _notify_message_counterpart(conv, f'{_user_display_name(current_user.id)}: {text[:80] if text else "файл"}')
    save_all()
    return jsonify({'ok': True, 'messages': _conversation_payload(conv), 'count': len(conv.get('messages', []))})


@app.route('/applications/<int:app_id>/status', methods=['POST'])
@login_required
@role_required('customer')
def application_status(app_id: int):
    app_item = next((a for a in applications if a['id'] == app_id), None)
    if not app_item:
        abort(404)

    order = _get_order(app_item['order_id'])
    if order.get('owner') != current_user.id:
        abort(403)

    new_status = request.form.get('status')
    if new_status not in {'accepted', 'declined'}:
        abort(400)

    if new_status == 'accepted':
        if order.get('status') not in {'open', 'has_responses'}:
            flash('Исполнителя можно выбрать только для открытого заказа.', 'warning')
            return redirect(url_for('my_orders'))
        if order.get('payment_status') != 'funded':
            flash('Сначала нужно пополнить бюджет объявления. После этого можно выбрать исполнителя.', 'warning')
            return redirect(url_for('order_detail', order_id=order['id']))
        if not app_item.get('customer_opened_chat'):
            flash('Перед подтверждением исполнителя сначала откройте чат и обсудите детали заказа.', 'warning')
            return redirect(url_for('my_orders'))

        app_item['status'] = 'accepted'
        order['status'] = 'in_progress'
        order['executor'] = app_item['username']
        order['payment_status'] = 'reserved'
        order['reserved_for'] = app_item['username']
        order['reserved_amount'] = order['reserve_amount']
        order['reserved_at'] = _now_iso()
        order['accepted_at'] = _now_iso()

        executor_wallet = _ensure_wallet(app_item['username'])
        executor_wallet['reserved'] += order['reserve_amount']
        _wallet_operation(app_item['username'], 'reserve', order['reserve_amount'], f'Зарезервировано 50% по заказу #{order["id"]}', order['id'])

        _notify(
            app_item['username'],
            'Отклик принят',
            f'Вы выбраны исполнителем заказа #{order["id"]}. 50% бюджета зарезервировано на балансе.',
            url_for('responses'),
            'success',
        )

        for other in applications:
            if other['order_id'] == order['id'] and other['id'] != app_item['id']:
                other['status'] = 'declined'
                _notify(
                    other.get('username'),
                    'Отклик отклонён',
                    f'По заказу #{order["id"]} выбран другой исполнитель.',
                    url_for('responses'),
                    'secondary',
                )
    else:
        app_item['status'] = 'declined'
        _notify(
            app_item.get('username'),
            'Отклик отклонён',
            f'Ваш отклик по заказу #{order["id"]} отклонён.',
            url_for('responses'),
            'secondary',
        )
        if order.get('executor') == app_item['username']:
            order['executor'] = None
        has_pending = any(a['order_id'] == order['id'] and a['status'] == 'pending' for a in applications)
        has_accepted = any(a['order_id'] == order['id'] and a['status'] == 'accepted' for a in applications)
        if has_accepted:
            order['status'] = 'in_progress'
        elif has_pending:
            order['status'] = 'has_responses'
        else:
            order['status'] = 'open'

    conv_id = app_item.get('conversation_id')
    if conv_id:
        conv = _get_conversation(conv_id)
        status_text = 'принят на выполнение. На балансе исполнителя зарезервировано 50% бюджета.' if new_status == 'accepted' else 'отклонён'
        conv['messages'].append({'sender': 'system', 'text': f'Статус отклика: {status_text}.', 'created_at': _now_iso(), 'kind': 'system'})

    save_all()
    if new_status == 'accepted':
        flash(f'Отклик {_user_display_name(app_item["username"])} на заказ #{order["id"]} принят. 50% бюджета зарезервировано исполнителю.', 'success')
    else:
        flash(f'Отклик {_user_display_name(app_item["username"])} на заказ #{order["id"]} отклонён.', 'secondary')
    return redirect(url_for('my_orders'))


@app.route('/orders/<int:order_id>/submit-review', methods=['POST'])
@login_required
@role_required('student')
def order_submit_review(order_id: int):
    order = _get_order(order_id)
    if order.get('executor') != current_user.id:
        abort(403)
    if order.get('status') != 'in_progress':
        flash('Отправить на проверку можно только заказ в работе.', 'warning')
        return redirect(url_for('order_detail', order_id=order_id))
    if order.get('payment_status') != 'reserved':
        flash('Работу можно отправить на проверку только после резервирования 50% бюджета на балансе исполнителя.', 'warning')
        return redirect(url_for('order_detail', order_id=order_id))

    result_url = (request.form.get('result_url') or '').strip()
    result_comment = (request.form.get('result_comment') or '').strip()
    if not result_url:
        flash('Укажите ссылку на выполненную работу.', 'warning')
        return redirect(url_for('order_detail', order_id=order_id))
    if not result_url.startswith(('http://', 'https://')):
        flash('Ссылка должна начинаться с http:// или https://', 'warning')
        return redirect(url_for('order_detail', order_id=order_id))
    if len(result_url) > 500:
        flash('Ссылка слишком длинная. Максимум 500 символов.', 'warning')
        return redirect(url_for('order_detail', order_id=order_id))
    if len(result_comment) > 2000:
        flash('Комментарий слишком длинный. Максимум 2000 символов.', 'warning')
        return redirect(url_for('order_detail', order_id=order_id))

    order['status'] = 'review'
    order['submitted_at'] = _now_iso()
    order['submitted_by'] = current_user.id
    order['submitted_url'] = result_url
    order['submitted_comment'] = result_comment

    system_text = f'Исполнитель отправил работу на проверку: {result_url}'
    if result_comment:
        system_text += f'\nКомментарий: {result_comment}'
    for conv in conversations:
        if conv.get('order_id') == order_id and current_user.id in (conv.get('student'), conv.get('customer')):
            conv['messages'].append({'sender': 'system', 'text': system_text, 'created_at': _now_iso(), 'kind': 'system'})

    _notify(
        order.get('owner'),
        'Работа отправлена на проверку',
        f'Исполнитель {_user_display_name(current_user.id)} отправил работу по заказу #{order_id}.',
        url_for('order_detail', order_id=order_id),
        'primary',
    )

    save_all()
    flash('Работа отправлена заказчику на проверку.', 'success')
    return redirect(url_for('order_detail', order_id=order_id))


@app.route('/orders/<int:order_id>/cancel', methods=['POST'])
@login_required
@role_required('customer')
def order_cancel(order_id: int):
    order = _get_order(order_id)
    if order.get('owner') != current_user.id:
        abort(403)
    if order.get('status') not in {'open', 'has_responses'}:
        flash('Открытый заказ можно отменить только до выбора исполнителя. Для активного заказа используйте отдельную кнопку отмены.', 'warning')
        return redirect(url_for('order_detail', order_id=order_id))

    order['status'] = 'cancelled'
    order['cancelled_at'] = _now_iso()
    order['cancel_reason'] = (request.form.get('reason') or 'Заказ отменён заказчиком до выбора исполнителя.').strip()

    if order.get('payment_status') == 'funded':
        order['payment_status'] = 'refunded'
        order['refunded_at'] = _now_iso()
        order['payment_history'].insert(0, {
            'kind': 'refund',
            'amount': int(order.get('funded_amount') or 0),
            'description': 'Возврат бюджета заказчику при отмене заказа',
            'created_at': _now_iso(),
        })
        _refund_order_budget_to_customer(order, f'Возврат бюджета по отменённому заказу #{order["id"]}')
        customer_wallet = _ensure_wallet(current_user.id)
        customer_wallet['customer_frozen'] = max(0, int(customer_wallet.get('customer_frozen', 0)) - int(order.get('funded_amount') or 0))

    for app_item in applications:
        if app_item.get('order_id') == order_id and app_item.get('status') == 'pending':
            app_item['status'] = 'cancelled'
            _notify(app_item.get('username'), 'Заказ отменён', f'Заказ #{order_id} отменён заказчиком.', url_for('responses'), 'warning')
            conv_id = app_item.get('conversation_id')
            if conv_id:
                conv = _get_conversation(conv_id)
                conv['messages'].append({'sender': 'system', 'text': 'Заказ отменён заказчиком.', 'created_at': _now_iso(), 'kind': 'system'})

    save_all()
    flash(f'Заказ #{order_id} отменён. Бюджет возвращён на баланс заказчика, если он был пополнен.', 'info')
    return redirect(url_for('my_orders'))


@app.route('/orders/<int:order_id>/cancel-active', methods=['POST'])
@login_required
def order_cancel_active(order_id: int):
    order = _get_order(order_id)
    role = get_current_role()

    if order.get('status') not in {'in_progress', 'review'}:
        flash('Отменить можно только активный заказ.', 'warning')
        return redirect(url_for('order_detail', order_id=order_id))

    reason = (request.form.get('reason') or '').strip()
    if len(reason) > 1000:
        flash('Причина отмены слишком длинная.', 'warning')
        return redirect(url_for('order_detail', order_id=order_id))

    if role == 'customer' and order.get('owner') == current_user.id:
        if not _is_order_deadline_passed(order):
            flash(f'Заказчик может отменить активный заказ с возвратом бюджета только после истечения срока: {_deadline_text(order)}.', 'warning')
            return redirect(url_for('order_detail', order_id=order_id))
        order['status'] = 'cancelled_by_customer'
        order['cancelled_at'] = _now_iso()
        order['cancel_reason'] = reason or 'Заказ отменён заказчиком после истечения срока.'
        _remove_executor_reserve(order)
        _refund_order_budget_to_customer(order, f'Возврат бюджета по заказу #{order["id"]}, отменённому заказчиком')
        _ensure_wallet(order.get('owner'))['customer_frozen'] = max(0, int(_ensure_wallet(order.get('owner')).get('customer_frozen', 0)) - int(order.get('funded_amount') or 0))
        order['payment_status'] = 'refunded'
        order['refunded_at'] = _now_iso()
        order['payment_history'].insert(0, {'kind': 'refund', 'amount': int(order.get('funded_amount') or 0), 'description': 'Возврат бюджета заказчику после отмены активного заказа', 'created_at': _now_iso()})
        _notify(order.get('executor'), 'Заказ отменён', f'Заказ #{order_id} отменён заказчиком после истечения срока.', url_for('responses'), 'warning')
        flash('Активный заказ отменён. Бюджет возвращён на баланс заказчика.', 'success')

    elif role == 'student' and order.get('executor') == current_user.id:
        order['status'] = 'cancelled_by_student'
        order['cancelled_at'] = _now_iso()
        order['cancel_reason'] = reason or 'Исполнитель отказался от выполнения заказа.'
        _remove_executor_reserve(order)
        _refund_order_budget_to_customer(order, f'Возврат бюджета по заказу #{order["id"]}: исполнитель отказался')
        _ensure_wallet(order.get('owner'))['customer_frozen'] = max(0, int(_ensure_wallet(order.get('owner')).get('customer_frozen', 0)) - int(order.get('funded_amount') or 0))
        order['payment_status'] = 'refunded'
        order['refunded_at'] = _now_iso()
        order['payment_history'].insert(0, {'kind': 'refund', 'amount': int(order.get('funded_amount') or 0), 'description': 'Возврат бюджета заказчику после отказа исполнителя', 'created_at': _now_iso()})
        _notify(order.get('owner'), 'Исполнитель отказался', f'Исполнитель отказался от заказа #{order_id}. Бюджет возвращён на баланс.', url_for('my_orders'), 'warning')
        flash('Вы отказались от выполнения заказа. Резерв снят, заказчик получил возврат бюджета.', 'info')
    else:
        abort(403)

    for conv in conversations:
        if conv.get('order_id') == order_id:
            conv['messages'].append({'sender': 'system', 'text': f'Заказ отменён. Причина: {order.get("cancel_reason", "не указана")}', 'created_at': _now_iso(), 'kind': 'system'})

    save_all()
    return redirect(url_for('order_detail', order_id=order_id))



@app.route('/orders/<int:order_id>/dispute', methods=['POST'])
@login_required
def order_open_dispute(order_id: int):
    order = _get_order(order_id)
    role = get_current_role()

    if order.get('status') not in {'in_progress', 'review'}:
        flash('Спор можно открыть только по активному заказу или заказу на проверке.', 'warning')
        return redirect(url_for('order_detail', order_id=order_id))

    if role == 'customer' and order.get('owner') == current_user.id:
        counterpart = order.get('executor')
        opener_role_text = 'заказчик'
    elif role == 'student' and order.get('executor') == current_user.id:
        counterpart = order.get('owner')
        opener_role_text = 'исполнитель'
    else:
        abort(403)

    reason = (request.form.get('reason') or '').strip()
    if not reason:
        flash('Укажите причину спора.', 'warning')
        return redirect(url_for('order_detail', order_id=order_id))

    if len(reason) > 1000:
        flash('Причина спора слишком длинная. Максимум 1000 символов.', 'warning')
        return redirect(url_for('order_detail', order_id=order_id))

    order['status'] = 'dispute'
    order['payment_status'] = 'disputed'
    order['dispute_opened_at'] = _now_iso()
    order['dispute_opened_by'] = current_user.id
    order['dispute_opened_role'] = role
    order['dispute_reason'] = reason

    order.setdefault('payment_history', [])
    order['payment_history'].insert(0, {
        'kind': 'dispute',
        'amount': int(order.get('funded_amount') or order.get('price') or 0),
        'description': f'Открыт спор по заказу #{order["id"]}',
        'created_at': _now_iso(),
    })

    opener_name = _user_display_name(current_user.id)
    system_text = f'{opener_name} открыл спор по заказу. Причина: {reason}'
    for conv in conversations:
        if conv.get('order_id') == order_id:
            conv['messages'].append({
                'sender': 'system',
                'text': system_text,
                'created_at': _now_iso(),
                'kind': 'system',
            })

    _notify(
        counterpart,
        'Открыт спор',
        f'{opener_name} открыл спор по заказу #{order_id}. Деньги заморожены до решения.',
        url_for('order_detail', order_id=order_id),
        'danger',
    )

    save_all()

    flash('Спор открыт. Средства по заказу заморожены до решения администратора.', 'warning')
    return redirect(url_for('order_detail', order_id=order_id))


@app.route('/orders/<int:order_id>/close', methods=['POST'])
@login_required
@role_required('customer')
def order_close(order_id: int):
    order = _get_order(order_id)
    if order.get('owner') != current_user.id:
        abort(403)
    if order.get('status') != 'review':
        flash('Принять работу можно только после отправки на проверку.', 'warning')
        return redirect(url_for('my_orders'))
    if order.get('payment_status') != 'reserved':
        flash('Нельзя завершить заказ без резерва средств.', 'warning')
        return redirect(url_for('my_orders'))

    executor = order.get('executor')
    if not executor:
        flash('У заказа нет исполнителя.', 'warning')
        return redirect(url_for('my_orders'))

    price = int(order.get('price') or 0)
    reserve_amount = int(order.get('reserved_amount') or order.get('reserve_amount') or 0)
    executor_wallet = _ensure_wallet(executor)
    executor_wallet['reserved'] = max(0, int(executor_wallet.get('reserved', 0)) - reserve_amount)
    executor_wallet['available'] += price
    _wallet_operation(executor, 'income', price, f'Начислена полная оплата по заказу #{order["id"]}', order['id'])
    customer_wallet = _ensure_wallet(order.get('owner'))
    customer_wallet['customer_frozen'] = max(0, int(customer_wallet.get('customer_frozen', 0)) - price)

    order['status'] = 'done'
    order['completed_at'] = _now_iso()
    order['payment_status'] = 'paid'
    order['paid_amount'] = price
    order['paid_at'] = _now_iso()
    order['payment_history'].insert(0, {'kind': 'paid', 'amount': price, 'description': f'Полная сумма начислена исполнителю {_user_display_name(executor)}', 'created_at': _now_iso()})

    for conv in conversations:
        if conv.get('order_id') == order_id:
            conv['messages'].append({'sender': 'system', 'text': 'Заказчик принял работу. Заказ завершён. Полная сумма начислена исполнителю.', 'created_at': _now_iso(), 'kind': 'system'})

    _notify(
        executor,
        'Работа принята',
        f'Заказ #{order_id} завершён. На баланс начислено {_money(price)} ₽.',
        url_for('balance'),
        'success',
    )

    save_all()
    flash(f'Заказ #{order_id} завершён. Исполнителю начислено {_money(price)} ₽.', 'success')
    return redirect(url_for('my_orders'))


@app.route('/profile', methods=['GET', 'POST'])
@login_required
def profile():
    profile_data = _get_profile(current_user.id)
    form = ProfileForm(skills=profile_data.get('skills', ''), about=profile_data.get('about', ''))
    if form.validate_on_submit():
        profile_data['skills'] = (form.skills.data or '').strip()
        profile_data['about'] = (form.about.data or '').strip()
        f = form.resume.data
        if f and f.filename:
            original = secure_filename(f.filename or '')
            ext = (original.rsplit('.', 1)[-1].lower() if '.' in original else '')
            base = _slugify(profile_data.get('display_name') or current_user.id)
            filename = f'{current_user.id}-{base}-{int(datetime.now(timezone.utc).timestamp())}.{ext}'
            f.save(os.path.join(RESUME_UPLOAD_FOLDER, filename))
            profile_data['resume_filename'] = filename
        profile_data['updated_at'] = _now_iso()
        save_all()
        flash('Профиль сохранён.', 'success')
        return redirect(url_for('profile'))
    return render_template('profile.html', form=form, profile=profile_data)


@app.route('/avatar/<path:filename>')
def avatar_download(filename: str):
    return send_from_directory(AVATAR_UPLOAD_FOLDER, filename)


@app.route('/u/<username>')
def public_profile(username: str):
    profile_data = _get_profile(username)
    rating_data = _get_user_rating(username)
    user_data = users.get(username) or {}
    role = user_data.get('role', 'student')

    completed_as_executor = [
        _ensure_order_finance(o)
        for o in orders
        if o.get('executor') == username and o.get('status') == 'done'
    ]
    active_as_executor = [
        _ensure_order_finance(o)
        for o in orders
        if o.get('executor') == username and o.get('status') in {'in_progress', 'review'}
    ]
    created_orders = [_ensure_order_finance(o) for o in orders if o.get('owner') == username]
    completed_created_orders = [o for o in created_orders if o.get('status') == 'done']

    if role == 'customer':
        profile_orders = created_orders
        history_title = 'История созданных заказов'
    else:
        profile_orders = completed_as_executor
        history_title = 'Выполненные задания'

    profile_orders = sorted(profile_orders, key=lambda o: int(o.get('id') or 0), reverse=True)

    profile_stats = {
        'completed_as_executor': len(completed_as_executor),
        'active_as_executor': len(active_as_executor),
        'created_orders': len(created_orders),
        'completed_created_orders': len(completed_created_orders),
        'reviews_count': rating_data['count'],
        'avg_rating': rating_data['avg'],
    }

    orders_by_id = {int(o.get('id') or 0): o for o in orders}

    student_info = _ensure_student_info(username) if role == 'student' else {}

    return render_template(
        'profile_public.html',
        owner=username,
        profile=profile_data,
        rating=rating_data,
        role=role,
        student_info=student_info,
        profile_stats=profile_stats,
        profile_orders=profile_orders,
        history_title=history_title,
        orders_by_id=orders_by_id,
    )


@app.route('/orders/<int:order_id>/review', methods=['GET', 'POST'])
@login_required
def leave_review(order_id: int):
    order = _get_order(order_id)
    role = get_current_role()

    if order.get('status') != 'done':
        flash('Отзыв можно оставить только после завершения заказа.', 'warning')
        return redirect(url_for('order_detail', order_id=order_id))

    if role == 'customer':
        if order.get('owner') != current_user.id:
            flash('Вы можете оставить отзыв только по своим заказам.', 'warning')
            return redirect(url_for('index'))
        target_username = order.get('executor')
        target_role = 'student'
        target_label = 'исполнителю'
        back_url = url_for('my_orders')
        placeholder = 'Расскажите, как исполнитель справился с заданием, сроками и коммуникацией...'
    elif role == 'student':
        if order.get('executor') != current_user.id:
            flash('Вы можете оставить отзыв только по заказу, который выполняли.', 'warning')
            return redirect(url_for('responses'))
        target_username = order.get('owner')
        target_role = 'customer'
        target_label = 'заказчику'
        back_url = url_for('responses')
        placeholder = 'Расскажите, насколько понятно заказчик поставил задачу и как проходила коммуникация...'
    else:
        flash('Недостаточно прав для отзыва.', 'warning')
        return redirect(url_for('index'))

    if not target_username:
        flash('Не найден пользователь для оценки.', 'warning')
        return redirect(back_url)

    if target_username == current_user.id:
        flash('Нельзя оставить отзыв самому себе.', 'warning')
        return redirect(back_url)

    if _has_review(order_id, current_user.id, target_username):
        flash('Вы уже оставили отзыв по этому заказу.', 'info')
        return redirect(url_for('public_profile', username=target_username))

    form = ReviewForm()
    if form.validate_on_submit():
        reviews.append({
            'id': len(reviews) + 1,
            'target_username': target_username,
            'target_role': target_role,
            # Поле оставлено для совместимости со старыми отзывами по студентам.
            'student_username': target_username if target_role == 'student' else order.get('executor'),
            'reviewer_username': current_user.id,
            'reviewer_role': role,
            'order_id': order_id,
            'rating': int(form.rating.data),
            'text': form.text.data.strip(),
            'created_at': _now_iso(),
        })
        save_all()
        flash('Отзыв сохранён!', 'success')
        return redirect(url_for('public_profile', username=target_username))

    return render_template(
        'review_form.html',
        form=form,
        order=order,
        target_username=target_username,
        target_role=target_role,
        target_label=target_label,
        back_url=back_url,
        placeholder=placeholder,
    )


@app.route('/resume/<path:filename>')
@login_required
def resume_download(filename: str):
    u = users.get(current_user.id) or {}
    profile_data = u.get('profile') or {}
    if profile_data.get('resume_filename') != filename:
        abort(403)
    return send_from_directory(RESUME_UPLOAD_FOLDER, filename, as_attachment=True)


@app.route('/chat-file/<path:filename>')
@login_required
def chat_file(filename: str):
    return send_from_directory(CHAT_UPLOAD_FOLDER, filename, as_attachment=True)


@app.route('/balance')
@login_required
def balance():
    wallet = _ensure_wallet(current_user.id)
    role = get_current_role()
    customer_orders = [_ensure_order_finance(o) for o in orders if o.get('owner') == current_user.id]
    customer_stats = {
        'funded_total': sum(int(o.get('fund_total_amount') or 0) for o in customer_orders if o.get('payment_status') in {'funded', 'reserved', 'paid', 'refunded'}),
        'fee_total': sum(int(o.get('platform_fee_amount') or 0) for o in customer_orders if o.get('payment_status') in {'funded', 'reserved', 'paid', 'refunded'}),
        'budget_total': sum(int(o.get('funded_amount') or 0) for o in customer_orders if o.get('payment_status') in {'funded', 'reserved', 'paid', 'refunded'}),
        'frozen_total': sum(int(o.get('funded_amount') or 0) for o in customer_orders if o.get('payment_status') in {'funded', 'reserved'}),
        'returned_total': int(wallet.get('customer_returned', 0)),
    }
    return render_template(
        'balance.html',
        wallet=wallet,
        fee_percent=SERVICE_FEE_PERCENT,
        role=role,
        customer_orders=customer_orders,
        customer_stats=customer_stats,
    )


@app.route('/balance/withdraw', methods=['POST'])
@login_required
def balance_withdraw():
    wallet = _ensure_wallet(current_user.id)
    try:
        amount = int(request.form.get('amount') or 0)
    except ValueError:
        amount = 0
    available = int(wallet.get('available', 0))
    if amount <= 0:
        flash('Введите корректную сумму вывода.', 'warning')
        return redirect(url_for('balance'))
    if amount > available:
        flash('Недостаточно средств для вывода.', 'warning')
        return redirect(url_for('balance'))

    commission = 0 if get_current_role() == 'customer' else int(amount * SERVICE_FEE_PERCENT / 100)
    payout = amount - commission
    wallet['available'] -= amount
    wallet['withdrawn'] += payout
    wallet['commission_paid'] += commission
    wallet['operations'].insert(0, {'id': len(wallet['operations']) + 1, 'kind': 'withdraw', 'amount': amount, 'commission': commission, 'payout': payout, 'description': 'Демонстрационная заявка на вывод средств', 'created_at': _now_iso()})
    save_all()
    flash(f'Заявка на вывод создана. К получению: {_money(payout)} ₽, комиссия сервиса: {_money(commission)} ₽.', 'success')
    return redirect(url_for('balance'))


@app.route('/notifications')
@login_required
def notifications_page():
    notifications = _ensure_notifications(current_user.id)
    return render_template('notifications.html', notifications=notifications)


@app.route('/notifications/mark-read', methods=['POST'])
@login_required
def notifications_mark_read():
    for notification in _ensure_notifications(current_user.id):
        notification['is_read'] = True
    save_all()
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return {'ok': True}
    flash('Уведомления отмечены как прочитанные.', 'success')
    return redirect(url_for('notifications_page'))


@app.route('/notifications/api')
@login_required
def notifications_api():
    notifications = _ensure_notifications(current_user.id)
    unread = [n for n in notifications if not n.get('is_read')]
    return jsonify({'unread_count': len(unread), 'latest': notifications[:8]})


@app.route('/settings', methods=['GET', 'POST'])
@login_required
def settings():
    user_data = users.get(current_user.id)
    if not user_data:
        logout_user()
        flash('Пользователь не найден. Войдите заново.', 'warning')
        return redirect(url_for('login'))
    profile_data = user_data.setdefault('profile', {})
    account_form = AccountSettingsForm(prefix='account')
    password_form = PasswordChangeForm(prefix='password')

    if request.method == 'GET':
        account_form.display_name.data = profile_data.get('display_name') or current_user.id
        account_form.email.data = user_data.get('email') or ''
        account_form.city.data = profile_data.get('city') or ''
        account_form.telegram.data = profile_data.get('telegram') or ''
        account_form.github.data = profile_data.get('github') or ''
        account_form.portfolio.data = profile_data.get('portfolio') or ''

    if request.method == 'POST':
        form_type = request.form.get('form_type')
        if form_type == 'account' and account_form.validate_on_submit():
            email = account_form.email.data.strip().lower()
            if _email_exists_for_another_user(email, current_user.id):
                flash('Этот email уже используется другим пользователем.', 'danger')
                return redirect(url_for('settings'))
            avatar_file = account_form.avatar.data
            if avatar_file and avatar_file.filename:
                if not _is_allowed_avatar(avatar_file.filename):
                    flash('Аватарка должна быть в формате jpg, jpeg, png или webp.', 'danger')
                    return redirect(url_for('settings'))
                if _uploaded_file_size(avatar_file) > MAX_AVATAR_SIZE:
                    flash('Размер аватарки не должен превышать 2 МБ.', 'danger')
                    return redirect(url_for('settings'))
                old_avatar = profile_data.get('avatar_filename') or ''
                new_avatar = _save_avatar(avatar_file, current_user.id)
                profile_data['avatar_filename'] = new_avatar
                if old_avatar and old_avatar != new_avatar:
                    _delete_avatar_file(old_avatar)
            user_data['email'] = email
            profile_data['display_name'] = account_form.display_name.data.strip()
            profile_data['city'] = (account_form.city.data or '').strip()
            profile_data['telegram'] = (account_form.telegram.data or '').strip()
            profile_data['github'] = (account_form.github.data or '').strip()
            profile_data['portfolio'] = (account_form.portfolio.data or '').strip()
            profile_data['updated_at'] = _now_iso()
            save_all()
            flash('Настройки аккаунта сохранены.', 'success')
            return redirect(url_for('settings'))
        if form_type == 'password' and password_form.validate_on_submit():
            if not check_password_hash(user_data['password'], password_form.current_password.data):
                flash('Текущий пароль указан неверно.', 'danger')
                return redirect(url_for('settings'))
            user_data['password'] = generate_password_hash(password_form.new_password.data)
            save_all()
            flash('Пароль успешно изменён.', 'success')
            return redirect(url_for('settings'))
        flash('Проверьте правильность заполнения формы.', 'danger')

    return render_template('settings.html', account_form=account_form, password_form=password_form)


@app.route('/support', methods=['GET', 'POST'])
@login_required
def support_page():
    if request.method == 'POST':
        category = (request.form.get('category') or 'other').strip()
        subject = (request.form.get('subject') or '').strip()
        message = (request.form.get('message') or '').strip()

        if category not in {'payment', 'order', 'student', 'customer', 'technical', 'dispute', 'other'}:
            category = 'other'
        if len(subject) < 4 or len(subject) > 140:
            flash('Тема обращения должна быть от 4 до 140 символов.', 'warning')
            return redirect(url_for('support_page'))
        if len(message) < 10 or len(message) > 3000:
            flash('Сообщение должно быть от 10 до 3000 символов.', 'warning')
            return redirect(url_for('support_page'))

        ticket_id = max([int(t.get('id') or 0) for t in support_tickets] or [0]) + 1
        ticket = {
            'id': ticket_id,
            'user': current_user.id,
            'user_name': _user_display_name(current_user.id),
            'category': category,
            'subject': subject,
            'message': message,
            'status': 'new',
            'admin_answer': '',
            'admin_username': '',
            'created_at': _now_iso(),
            'updated_at': _now_iso(),
            'closed_at': '',
        }
        support_tickets.insert(0, ticket)
        _notify_admins(
            'Новое обращение в поддержку',
            f'{_user_display_name(current_user.id)} написал в поддержку: {subject}',
            url_for('admin_support'),
            'info',
        )
        save_all()
        flash('Обращение отправлено в поддержку. Ответ появится здесь и в уведомлениях.', 'success')
        return redirect(url_for('support_page'))

    my_tickets = [t for t in support_tickets if t.get('user') == current_user.id]
    return render_template('support.html', tickets=my_tickets)


@app.route('/admin')
@login_required
@admin_required
def admin_dashboard():
    recent_tickets = support_tickets[:5]
    recent_orders = sorted([_ensure_order_finance(o) for o in orders], key=lambda o: int(o.get('id') or 0), reverse=True)[:8]
    dispute_orders = [_ensure_order_finance(o) for o in orders if o.get('status') == 'dispute']
    return render_template(
        'admin_dashboard.html',
        stats=_get_admin_stats(),
        recent_tickets=recent_tickets,
        recent_orders=recent_orders,
        dispute_orders=dispute_orders,
    )


@app.route('/admin/users')
@login_required
@admin_required
def admin_users():
    user_rows = []
    for username, data in sorted(users.items(), key=lambda item: item[0].lower()):
        wallet = _ensure_wallet(username)
        role = data.get('role') or 'student'
        user_rows.append({
            'username': username,
            'display_name': _user_display_name(username),
            'email': data.get('email') or '',
            'role': role,
            'is_blocked': bool(data.get('is_blocked')),
            'wallet': wallet,
            'orders_created': len([o for o in orders if o.get('owner') == username]),
            'orders_executed': len([o for o in orders if o.get('executor') == username]),
            'responses_count': len([a for a in applications if a.get('username') == username]),
            'rating': _get_user_rating(username),
        })
    return render_template('admin_users.html', user_rows=user_rows)


@app.route('/admin/users/<username>/role', methods=['POST'])
@login_required
@admin_required
def admin_user_role(username: str):
    if username not in users:
        abort(404)
    new_role = request.form.get('role')
    if new_role not in {'student', 'customer', 'admin'}:
        flash('Некорректная роль пользователя.', 'warning')
        return redirect(url_for('admin_users'))
    if username == current_user.id and new_role != 'admin':
        flash('Нельзя снять роль администратора с самого себя.', 'warning')
        return redirect(url_for('admin_users'))
    users[username]['role'] = new_role
    if new_role == 'student':
        _ensure_student_info(username)
    _notify(username, 'Роль аккаунта изменена', f'Администратор изменил вашу роль на: {new_role}.', url_for('index'), 'info')
    save_all()
    flash(f'Роль пользователя {_user_display_name(username)} обновлена.', 'success')
    return redirect(url_for('admin_users'))


@app.route('/admin/users/<username>/block', methods=['POST'])
@login_required
@admin_required
def admin_user_block(username: str):
    if username not in users:
        abort(404)
    if username == current_user.id:
        flash('Нельзя заблокировать самого себя.', 'warning')
        return redirect(url_for('admin_users'))
    action = request.form.get('action')
    users[username]['is_blocked'] = action == 'block'
    if users[username]['is_blocked']:
        users[username]['blocked_at'] = _now_iso()
        users[username]['blocked_by'] = current_user.id
        _notify(username, 'Аккаунт заблокирован', 'Администратор ограничил доступ к аккаунту.', None, 'danger')
        flash('Пользователь заблокирован.', 'warning')
    else:
        users[username]['unblocked_at'] = _now_iso()
        users[username]['unblocked_by'] = current_user.id
        _notify(username, 'Аккаунт разблокирован', 'Администратор восстановил доступ к аккаунту.', url_for('index'), 'success')
        flash('Пользователь разблокирован.', 'success')
    save_all()
    return redirect(url_for('admin_users'))


@app.route('/admin/orders')
@login_required
@admin_required
def admin_orders():
    all_orders = sorted([_ensure_order_finance(o) for o in orders], key=lambda o: int(o.get('id') or 0), reverse=True)
    return render_template('admin_orders.html', orders=all_orders)


@app.route('/admin/disputes')
@login_required
@admin_required
def admin_disputes():
    dispute_orders = sorted([_ensure_order_finance(o) for o in orders if o.get('status') == 'dispute'], key=lambda o: int(o.get('id') or 0), reverse=True)
    conversations_by_order = {c.get('order_id'): c for c in conversations}
    return render_template('admin_disputes.html', dispute_orders=dispute_orders, conversations_by_order=conversations_by_order)


@app.route('/admin/disputes/<int:order_id>/resolve', methods=['POST'])
@login_required
@admin_required
def admin_resolve_dispute(order_id: int):
    order = _get_order(order_id)
    if order.get('status') != 'dispute':
        flash('Этот заказ сейчас не находится в споре.', 'warning')
        return redirect(url_for('admin_disputes'))

    decision = request.form.get('decision')
    note = (request.form.get('note') or '').strip()
    customer = order.get('owner')
    executor = order.get('executor')
    price = int(order.get('price') or 0)
    reserve_amount = int(order.get('reserved_amount') or order.get('reserve_amount') or 0)

    if decision == 'customer':
        _remove_executor_reserve(order)
        _refund_order_budget_to_customer(order, f'Возврат бюджета по спору заказа #{order["id"]} в пользу заказчика')
        if customer:
            customer_wallet = _ensure_wallet(customer)
            customer_wallet['customer_frozen'] = max(0, int(customer_wallet.get('customer_frozen', 0)) - int(order.get('funded_amount') or price))
        order['status'] = 'cancelled_by_student'
        order['payment_status'] = 'refunded'
        order['refunded_at'] = _now_iso()
        result_text = 'Спор закрыт в пользу заказчика. Бюджет возвращён заказчику, резерв исполнителя снят.'
        _notify(customer, 'Спор решён в вашу пользу', f'По заказу #{order_id} бюджет возвращён на баланс.', url_for('balance'), 'success')
        _notify(executor, 'Спор закрыт', f'По заказу #{order_id} решение принято в пользу заказчика.', url_for('responses'), 'warning')
    elif decision == 'student':
        if executor:
            executor_wallet = _ensure_wallet(executor)
            executor_wallet['reserved'] = max(0, int(executor_wallet.get('reserved', 0)) - reserve_amount)
            executor_wallet['available'] += price
            _wallet_operation(executor, 'income', price, f'Начислена оплата по решению спора заказа #{order["id"]}', order['id'])
        if customer:
            customer_wallet = _ensure_wallet(customer)
            customer_wallet['customer_frozen'] = max(0, int(customer_wallet.get('customer_frozen', 0)) - price)
        order['status'] = 'done'
        order['payment_status'] = 'paid'
        order['paid_amount'] = price
        order['paid_at'] = _now_iso()
        order['completed_at'] = _now_iso()
        result_text = 'Спор закрыт в пользу исполнителя. Полная сумма начислена исполнителю.'
        _notify(executor, 'Спор решён в вашу пользу', f'По заказу #{order_id} начислено {_money(price)} ₽.', url_for('balance'), 'success')
        _notify(customer, 'Спор закрыт', f'По заказу #{order_id} решение принято в пользу исполнителя.', url_for('my_orders'), 'warning')
    else:
        flash('Выберите решение по спору.', 'warning')
        return redirect(url_for('admin_disputes'))

    order['dispute_resolved_at'] = _now_iso()
    order['dispute_resolved_by'] = current_user.id
    order['dispute_resolution'] = decision
    order['dispute_resolution_note'] = note
    order.setdefault('payment_history', [])
    order['payment_history'].insert(0, {
        'kind': 'dispute_resolved',
        'amount': price,
        'description': result_text + (f' Комментарий: {note}' if note else ''),
        'created_at': _now_iso(),
    })

    for app_item in applications:
        if app_item.get('order_id') == order_id and app_item.get('status') == 'accepted' and decision == 'customer':
            app_item['status'] = 'cancelled'

    for conv in conversations:
        if conv.get('order_id') == order_id:
            conv['messages'].append({
                'sender': 'system',
                'text': result_text + (f' Комментарий администратора: {note}' if note else ''),
                'created_at': _now_iso(),
                'kind': 'system',
            })

    save_all()
    flash('Решение по спору сохранено.', 'success')
    return redirect(url_for('admin_disputes'))


@app.route('/admin/support')
@login_required
@admin_required
def admin_support():
    status = (request.args.get('status') or '').strip()
    tickets = support_tickets
    if status in {'new', 'in_progress', 'closed'}:
        tickets = [t for t in tickets if t.get('status') == status]
    return render_template('admin_support.html', tickets=tickets, status=status)


@app.route('/admin/support/<int:ticket_id>', methods=['POST'])
@login_required
@admin_required
def admin_support_update(ticket_id: int):
    ticket = _get_ticket(ticket_id)
    status = request.form.get('status') or ticket.get('status') or 'new'
    answer = (request.form.get('answer') or '').strip()

    if status not in {'new', 'in_progress', 'closed'}:
        flash('Некорректный статус обращения.', 'warning')
        return redirect(url_for('admin_support'))
    if len(answer) > 3000:
        flash('Ответ поддержки слишком длинный.', 'warning')
        return redirect(url_for('admin_support'))

    old_status = ticket.get('status')
    ticket['status'] = status
    ticket['admin_username'] = current_user.id
    ticket['updated_at'] = _now_iso()
    if status == 'closed':
        ticket['closed_at'] = _now_iso()
    if answer:
        ticket['admin_answer'] = answer
        ticket['answered_at'] = _now_iso()

    user = ticket.get('user')
    if answer:
        _notify(user, 'Ответ поддержки', f'По обращению #{ticket_id}: {answer[:140]}', url_for('support_page'), 'success')
    elif old_status != status:
        _notify(user, 'Статус обращения изменён', f'Обращение #{ticket_id}: {_support_status_text(status)}.', url_for('support_page'), 'info')

    save_all()
    flash('Обращение обновлено.', 'success')
    return redirect(url_for('admin_support'))


@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('Вы вышли', 'info')
    return redirect(url_for('index'))



@app.errorhandler(403)
def forbidden_error(error):
    return render_template('403.html'), 403


@app.errorhandler(404)
def not_found_error(error):
    return render_template('404.html'), 404


@app.errorhandler(500)
def internal_error(error):
    return render_template('500.html'), 500


if __name__ == '__main__':
    app.run(debug=app.config['DEBUG'])
