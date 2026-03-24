#!/usr/bin/env python3
"""
Tovest Telegram Bot - Event & Check-in & Referral System
=========================================================
Bot quản lý event link, check-in hàng ngày, referral, quy đổi USDT.
Sử dụng: python-telegram-bot v20+ (async) + SQLite + JobQueue (APScheduler)

Author: Manus AI
"""

import os
import io
import csv
import logging
import sqlite3
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    BotCommand
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters
)
from telegram.constants import ParseMode

# ============================================================
# CẤU HÌNH
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "8602851516:AAFBNXYaMbe6ujdz42nXjUCrpeRQrebUKjw")
BOT_USERNAME = os.getenv("BOT_USERNAME", "testeventtovest_bot")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "leyleyeyy")
EVENT_LINK = "https://tovest.com/en-US?m=botevent&c=1600000005&ext=1"
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
DB_PATH = os.getenv("DB_PATH", "bot_data.db")

# Điểm check-in
BASE_POINTS = 10
STREAK_BONUS = 5  # +5 mỗi ngày streak
MILESTONE_BONUSES = {7: 50, 14: 120, 30: 300, 60: 700, 100: 1500}

# Quy đổi USDT
POINTS_PER_REDEEM = 500
USDT_PER_REDEEM = 0.05

# Group chat IDs lưu trong DB, tự động detect khi bot được add vào group
# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("ToveBot")

# ============================================================
# DATABASE
# ============================================================

def get_db() -> sqlite3.Connection:
    """Tạo kết nối SQLite với row_factory."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    """Khởi tạo tất cả bảng cần thiết."""
    conn = get_db()
    conn.executescript("""
        -- Bảng user
        CREATE TABLE IF NOT EXISTS users (
            user_id     INTEGER PRIMARY KEY,
            username    TEXT DEFAULT '',
            full_name   TEXT DEFAULT '',
            points      INTEGER DEFAULT 0,
            streak      INTEGER DEFAULT 0,
            last_checkin TEXT DEFAULT '',
            referred_by INTEGER DEFAULT 0,
            created_at  TEXT DEFAULT (datetime('now'))
        );

        -- Bảng check-in history
        CREATE TABLE IF NOT EXISTS checkins (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id  INTEGER NOT NULL,
            date     TEXT NOT NULL,
            points   INTEGER DEFAULT 0,
            streak   INTEGER DEFAULT 0,
            UNIQUE(user_id, date)
        );

        -- Bảng referral
        CREATE TABLE IF NOT EXISTS referrals (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id   INTEGER NOT NULL,
            referred_id   INTEGER NOT NULL,
            checkin_count INTEGER DEFAULT 0,
            rewarded      INTEGER DEFAULT 0,
            created_at    TEXT DEFAULT (datetime('now')),
            UNIQUE(referred_id)
        );

        -- Bảng event click tracking
        CREATE TABLE IF NOT EXISTS event_clicks (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id   INTEGER NOT NULL,
            username  TEXT DEFAULT '',
            clicked_at TEXT DEFAULT (datetime('now'))
        );

        -- Bảng group IDs
        CREATE TABLE IF NOT EXISTS groups (
            chat_id INTEGER PRIMARY KEY,
            title   TEXT DEFAULT '',
            added_at TEXT DEFAULT (datetime('now'))
        );

        -- Bảng quy đổi USDT
        CREATE TABLE IF NOT EXISTS redemptions (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id   INTEGER NOT NULL,
            points    INTEGER NOT NULL,
            usdt      REAL NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    conn.close()
    logger.info("Database đã khởi tạo thành công.")

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def is_admin(user) -> bool:
    """Kiểm tra user có phải admin không."""
    return user.username and user.username.lower() == ADMIN_USERNAME.lower()

def vn_today() -> str:
    """Ngày hôm nay theo giờ VN (YYYY-MM-DD)."""
    return datetime.now(VN_TZ).strftime("%Y-%m-%d")

def vn_now() -> datetime:
    """Thời gian hiện tại theo giờ VN."""
    return datetime.now(VN_TZ)

def get_or_create_user(user_id: int, username: str = "", full_name: str = "") -> dict:
    """Lấy hoặc tạo user mới trong DB."""
    conn = get_db()
    row = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
    if row:
        # Cập nhật username/full_name nếu thay đổi
        conn.execute(
            "UPDATE users SET username = ?, full_name = ? WHERE user_id = ?",
            (username, full_name, user_id)
        )
        conn.commit()
        row = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
    else:
        conn.execute(
            "INSERT INTO users (user_id, username, full_name) VALUES (?, ?, ?)",
            (user_id, username, full_name)
        )
        conn.commit()
        row = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
    result = dict(row)
    conn.close()
    return result

def save_group(chat_id: int, title: str = ""):
    """Lưu group chat ID vào DB."""
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO groups (chat_id, title) VALUES (?, ?)",
        (chat_id, title)
    )
    conn.commit()
    conn.close()

def get_all_groups() -> list:
    """Lấy tất cả group IDs."""
    conn = get_db()
    rows = conn.execute("SELECT chat_id FROM groups").fetchall()
    conn.close()
    return [r["chat_id"] for r in rows]

def get_all_users() -> list:
    """Lấy tất cả user IDs."""
    conn = get_db()
    rows = conn.execute("SELECT user_id FROM users").fetchall()
    conn.close()
    return [r["user_id"] for r in rows]

def display_name(user) -> str:
    """Tên hiển thị của user."""
    if user.full_name:
        return user.full_name
    if user.username:
        return f"@{user.username}"
    return f"User#{user.id}"

# ============================================================
# QUY TẮC
# ============================================================

RULES_TEXT = """
📋 <b>QUY TẮC CHƯƠNG TRÌNH TOVEST BOT</b>

<b>1. Check-in hàng ngày:</b>
• Mỗi ngày bấm nút ✅ Check-in để nhận điểm
• Mỗi user chỉ check-in được 1 lần/ngày
• Điểm cơ bản: <b>+10 điểm</b>
• Streak bonus: <b>+5 điểm</b> cho mỗi ngày liên tiếp
• Ví dụ: Ngày 1 = 10đ, Ngày 2 = 15đ, Ngày 3 = 20đ...

<b>2. Milestone bonus (thưởng cột mốc):</b>
• 7 ngày liên tiếp: <b>+50 điểm</b>
• 14 ngày liên tiếp: <b>+120 điểm</b>
• 30 ngày liên tiếp: <b>+300 điểm</b>
• 60 ngày liên tiếp: <b>+700 điểm</b>
• 100 ngày liên tiếp: <b>+1500 điểm</b>

<b>3. Mời bạn bè (Referral):</b>
• Chia sẻ link mời: t.me/{bot}?start=ref_USERID
• Bạn bè join group + check-in 3 ngày → bạn nhận <b>+10 điểm</b>

<b>4. Quy đổi USDT:</b>
• <b>500 điểm = 0.05 USDT</b>
• Trả vào tài khoản Tovest
• Thanh toán được xử lý mỗi thứ 2 hàng tuần

<b>5. Event link:</b>
• Bot tự động gửi link event vào 08:00, 12:00, 18:00, 21:00
• Bấm vào link để tham gia event trên Tovest

<b>6. Bảng xếp hạng:</b>
• Top user theo tổng điểm và streak dài nhất
• Cập nhật realtime qua lệnh /leaderboard
""".replace("{bot}", BOT_USERNAME)

# ============================================================
# COMMAND HANDLERS
# ============================================================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý /start - Đăng ký user, xử lý referral deep link."""
    user = update.effective_user
    if not user:
        return

    # Lưu group nếu gọi từ group
    chat = update.effective_chat
    if chat.type in ("group", "supergroup"):
        save_group(chat.id, chat.title or "")

    # Tạo/cập nhật user
    get_or_create_user(user.id, user.username or "", user.full_name or "")

    # Xử lý referral deep link: /start ref_12345
    if context.args and context.args[0].startswith("ref_"):
        try:
            referrer_id = int(context.args[0].replace("ref_", ""))
            if referrer_id != user.id:
                conn = get_db()
                # Kiểm tra chưa có referral
                existing = conn.execute(
                    "SELECT id FROM referrals WHERE referred_id = ?", (user.id,)
                ).fetchone()
                if not existing:
                    conn.execute(
                        "INSERT INTO referrals (referrer_id, referred_id) VALUES (?, ?)",
                        (referrer_id, user.id)
                    )
                    conn.execute(
                        "UPDATE users SET referred_by = ? WHERE user_id = ?",
                        (referrer_id, user.id)
                    )
                    conn.commit()
                    logger.info(f"Referral: {referrer_id} mời {user.id}")
                conn.close()
        except (ValueError, IndexError):
            pass

    text = (
        f"👋 Chào <b>{display_name(user)}</b>!\n\n"
        f"🤖 Chào mừng đến với <b>Tovest Bot</b>!\n\n"
        f"📌 Các lệnh chính:\n"
        f"/checkin - Check-in nhận điểm\n"
        f"/myinfo - Thông tin cá nhân\n"
        f"/myreferral - Link mời bạn bè\n"
        f"/leaderboard - Bảng xếp hạng\n"
        f"/rules - Quy tắc chương trình\n"
        f"/event - Xem event link\n\n"
        f"💡 Hãy check-in mỗi ngày để tích điểm!"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

async def cmd_checkin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý /checkin - Gửi nút check-in inline."""
    # Lưu group
    chat = update.effective_chat
    if chat.type in ("group", "supergroup"):
        save_group(chat.id, chat.title or "")

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Check-in ngay!", callback_data="checkin")],
        [InlineKeyboardButton("📋 Quy tắc", callback_data="rules")]
    ])
    await update.message.reply_text(
        "📅 <b>DAILY CHECK-IN</b>\n\n"
        "Bấm nút bên dưới để check-in hôm nay!\n"
        "🎯 +10 điểm cơ bản + streak bonus",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard
    )

async def callback_checkin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý callback khi user bấm nút Check-in."""
    query = update.callback_query
    await query.answer()
    user = query.from_user
    today = vn_today()

    # Tạo/cập nhật user
    u = get_or_create_user(user.id, user.username or "", user.full_name or "")

    conn = get_db()

    # Kiểm tra đã check-in hôm nay chưa
    existing = conn.execute(
        "SELECT id FROM checkins WHERE user_id = ? AND date = ?",
        (user.id, today)
    ).fetchone()
    if existing:
        conn.close()
        await query.answer("⚠️ Bạn đã check-in hôm nay rồi!", show_alert=True)
        return

    # Tính streak
    yesterday = (datetime.now(VN_TZ) - timedelta(days=1)).strftime("%Y-%m-%d")
    if u["last_checkin"] == yesterday:
        new_streak = u["streak"] + 1
    elif u["last_checkin"] == today:
        # Đã check-in (double check)
        conn.close()
        await query.answer("⚠️ Bạn đã check-in hôm nay rồi!", show_alert=True)
        return
    else:
        new_streak = 1

    # Tính điểm
    points = BASE_POINTS + STREAK_BONUS * (new_streak - 1)
    milestone_bonus = MILESTONE_BONUSES.get(new_streak, 0)
    total_points = points + milestone_bonus

    # Cập nhật DB
    conn.execute(
        "INSERT INTO checkins (user_id, date, points, streak) VALUES (?, ?, ?, ?)",
        (user.id, today, total_points, new_streak)
    )
    conn.execute(
        "UPDATE users SET points = points + ?, streak = ?, last_checkin = ? WHERE user_id = ?",
        (total_points, new_streak, today, user.id)
    )

    # Cập nhật referral checkin count
    ref = conn.execute(
        "SELECT * FROM referrals WHERE referred_id = ?", (user.id,)
    ).fetchone()
    if ref:
        new_count = ref["checkin_count"] + 1
        conn.execute(
            "UPDATE referrals SET checkin_count = ? WHERE referred_id = ?",
            (new_count, user.id)
        )
        # Nếu đủ 3 ngày check-in và chưa thưởng → thưởng người mời
        if new_count >= 3 and not ref["rewarded"]:
            conn.execute(
                "UPDATE users SET points = points + 10 WHERE user_id = ?",
                (ref["referrer_id"],)
            )
            conn.execute(
                "UPDATE referrals SET rewarded = 1 WHERE referred_id = ?",
                (user.id,)
            )
            logger.info(f"Referral reward: {ref['referrer_id']} nhận 10đ từ {user.id}")

    conn.commit()
    conn.close()

    # Tạo message kết quả
    text = (
        f"✅ <b>{display_name(user)}</b> đã check-in thành công!\n\n"
        f"📊 Streak: <b>{new_streak} ngày</b> 🔥\n"
        f"💰 Điểm nhận: <b>+{points}</b>"
    )
    if milestone_bonus:
        text += f"\n🎉 Milestone bonus ({new_streak} ngày): <b>+{milestone_bonus}</b>"
    text += f"\n📈 Tổng hôm nay: <b>+{total_points}</b>"

    # Lấy tổng điểm mới
    conn2 = get_db()
    row = conn2.execute("SELECT points FROM users WHERE user_id = ?", (user.id,)).fetchone()
    conn2.close()
    if row:
        text += f"\n\n💎 Tổng điểm: <b>{row['points']}</b>"

    await query.message.reply_text(text, parse_mode=ParseMode.HTML)

async def callback_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý callback khi user bấm nút Quy tắc."""
    query = update.callback_query
    await query.answer()
    await query.message.reply_text(RULES_TEXT, parse_mode=ParseMode.HTML)

async def cmd_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý /rules - Hiển thị quy tắc."""
    await update.message.reply_text(RULES_TEXT, parse_mode=ParseMode.HTML)

async def cmd_myinfo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý /myinfo - Thông tin cá nhân."""
    user = update.effective_user
    u = get_or_create_user(user.id, user.username or "", user.full_name or "")

    conn = get_db()
    total_checkins = conn.execute(
        "SELECT COUNT(*) as cnt FROM checkins WHERE user_id = ?", (user.id,)
    ).fetchone()["cnt"]
    total_referrals = conn.execute(
        "SELECT COUNT(*) as cnt FROM referrals WHERE referrer_id = ?", (user.id,)
    ).fetchone()["cnt"]
    total_redeemed = conn.execute(
        "SELECT COALESCE(SUM(usdt), 0) as total FROM redemptions WHERE user_id = ?",
        (user.id,)
    ).fetchone()["total"]
    conn.close()

    redeem_available = u["points"] // POINTS_PER_REDEEM

    text = (
        f"👤 <b>Thông tin của {display_name(user)}</b>\n\n"
        f"💎 Tổng điểm: <b>{u['points']}</b>\n"
        f"🔥 Streak hiện tại: <b>{u['streak']} ngày</b>\n"
        f"📅 Tổng ngày check-in: <b>{total_checkins}</b>\n"
        f"👥 Đã mời: <b>{total_referrals} người</b>\n"
        f"💵 Đã quy đổi: <b>{total_redeemed:.2f} USDT</b>\n"
        f"🔄 Có thể quy đổi: <b>{redeem_available} lần</b> "
        f"({redeem_available * USDT_PER_REDEEM:.2f} USDT)\n\n"
        f"📌 Check-in cuối: {u['last_checkin'] or 'Chưa check-in'}"
    )

    keyboard = None
    if redeem_available > 0:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                f"💵 Quy đổi {USDT_PER_REDEEM} USDT ({POINTS_PER_REDEEM}đ)",
                callback_data="redeem"
            )]
        ])

    await update.message.reply_text(
        text, parse_mode=ParseMode.HTML, reply_markup=keyboard
    )

async def callback_redeem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý quy đổi USDT."""
    query = update.callback_query
    await query.answer()
    user = query.from_user

    conn = get_db()
    u = conn.execute("SELECT * FROM users WHERE user_id = ?", (user.id,)).fetchone()

    if not u or u["points"] < POINTS_PER_REDEEM:
        conn.close()
        await query.answer(
            f"⚠️ Bạn cần ít nhất {POINTS_PER_REDEEM} điểm để quy đổi!",
            show_alert=True
        )
        return

    # Trừ điểm và ghi nhận
    conn.execute(
        "UPDATE users SET points = points - ? WHERE user_id = ?",
        (POINTS_PER_REDEEM, user.id)
    )
    conn.execute(
        "INSERT INTO redemptions (user_id, points, usdt) VALUES (?, ?, ?)",
        (user.id, POINTS_PER_REDEEM, USDT_PER_REDEEM)
    )
    conn.commit()

    new_points = conn.execute(
        "SELECT points FROM users WHERE user_id = ?", (user.id,)
    ).fetchone()["points"]
    conn.close()

    text = (
        f"💵 <b>Quy đổi thành công!</b>\n\n"
        f"• Đã trừ: <b>{POINTS_PER_REDEEM} điểm</b>\n"
        f"• Nhận: <b>{USDT_PER_REDEEM} USDT</b>\n"
        f"• Điểm còn lại: <b>{new_points}</b>\n\n"
        f"📌 USDT sẽ được trả vào tài khoản Tovest.\n"
        f"Thanh toán xử lý mỗi thứ 2 hàng tuần."
    )
    await query.message.reply_text(text, parse_mode=ParseMode.HTML)
    logger.info(f"Redeem: {user.id} đổi {POINTS_PER_REDEEM}đ → {USDT_PER_REDEEM} USDT")

async def cmd_myreferral(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý /myreferral - Link mời bạn bè."""
    user = update.effective_user
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{user.id}"
    text = (
        f"👥 <b>Link mời bạn bè của bạn:</b>\n\n"
        f"🔗 <code>{ref_link}</code>\n\n"
        f"📌 Chia sẻ link này cho bạn bè.\n"
        f"Khi bạn bè join + check-in 3 ngày → bạn nhận <b>+10 điểm</b>!"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

async def cmd_referral_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý /referral_info - Thống kê mời bạn bè."""
    user = update.effective_user
    conn = get_db()
    refs = conn.execute(
        "SELECT r.*, u.username, u.full_name FROM referrals r "
        "LEFT JOIN users u ON r.referred_id = u.user_id "
        "WHERE r.referrer_id = ? ORDER BY r.created_at DESC",
        (user.id,)
    ).fetchall()
    conn.close()

    if not refs:
        await update.message.reply_text(
            "📭 Bạn chưa mời ai. Dùng /myreferral để lấy link mời!"
        )
        return

    total = len(refs)
    qualified = sum(1 for r in refs if r["checkin_count"] >= 3)
    points_earned = qualified * 10

    text = f"👥 <b>Thống kê mời bạn bè</b>\n\n"
    text += f"📊 Tổng đã mời: <b>{total}</b>\n"
    text += f"✅ Đã đủ 3 ngày check-in: <b>{qualified}</b>\n"
    text += f"💰 Điểm từ referral: <b>{points_earned}</b>\n\n"
    text += "<b>Chi tiết:</b>\n"

    for i, r in enumerate(refs[:20], 1):
        name = r["full_name"] or r["username"] or f"User#{r['referred_id']}"
        status = "✅" if r["checkin_count"] >= 3 else f"⏳ {r['checkin_count']}/3"
        text += f"{i}. {name} - {status}\n"

    if total > 20:
        text += f"\n... và {total - 20} người khác"

    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

async def cmd_event(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý /event - Gửi event link."""
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎁 Tham gia Event Tovest", callback_data="event_click")],
    ])
    await update.message.reply_text(
        "🎉 <b>EVENT TOVEST</b>\n\n"
        "Bấm nút bên dưới để tham gia event và nhận thưởng!",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard
    )

async def callback_event_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Tracking click event link và gửi link cho user."""
    query = update.callback_query
    user = query.from_user

    # Ghi nhận click
    conn = get_db()
    conn.execute(
        "INSERT INTO event_clicks (user_id, username) VALUES (?, ?)",
        (user.id, user.username or "")
    )
    conn.commit()
    conn.close()

    await query.answer("🔗 Đang mở link event...", show_alert=False)
    await query.message.reply_text(
        f"🎁 <b>{display_name(user)}</b>, đây là link event:\n\n"
        f"👉 <a href=\"{EVENT_LINK}\">Tham gia Event Tovest</a>\n\n"
        f"📌 Mở link trên trình duyệt để tham gia!",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )

async def cmd_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý /leaderboard - Bảng xếp hạng."""
    conn = get_db()

    # Top điểm
    top_points = conn.execute(
        "SELECT user_id, username, full_name, points, streak "
        "FROM users ORDER BY points DESC LIMIT 10"
    ).fetchall()

    # Top streak
    top_streak = conn.execute(
        "SELECT user_id, username, full_name, points, streak "
        "FROM users WHERE streak > 0 ORDER BY streak DESC LIMIT 10"
    ).fetchall()
    conn.close()

    medals = ["🥇", "🥈", "🥉"]

    text = "🏆 <b>BẢNG XẾP HẠNG</b>\n\n"
    text += "<b>💎 Top Điểm:</b>\n"
    for i, u in enumerate(top_points):
        medal = medals[i] if i < 3 else f"{i+1}."
        name = u["full_name"] or u["username"] or f"User#{u['user_id']}"
        text += f"{medal} {name} - <b>{u['points']}đ</b> (🔥{u['streak']})\n"

    if not top_points:
        text += "Chưa có dữ liệu.\n"

    text += "\n<b>🔥 Top Streak:</b>\n"
    for i, u in enumerate(top_streak):
        medal = medals[i] if i < 3 else f"{i+1}."
        name = u["full_name"] or u["username"] or f"User#{u['user_id']}"
        text += f"{medal} {name} - <b>{u['streak']} ngày</b> ({u['points']}đ)\n"

    if not top_streak:
        text += "Chưa có dữ liệu.\n"

    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

# ============================================================
# ADMIN COMMANDS
# ============================================================

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: /stats - Thống kê click event."""
    if not is_admin(update.effective_user):
        await update.message.reply_text("⛔ Bạn không có quyền sử dụng lệnh này.")
        return

    conn = get_db()
    total = conn.execute("SELECT COUNT(*) as cnt FROM event_clicks").fetchone()["cnt"]
    today_cnt = conn.execute(
        "SELECT COUNT(*) as cnt FROM event_clicks WHERE date(clicked_at) = date('now')"
    ).fetchone()["cnt"]
    unique = conn.execute(
        "SELECT COUNT(DISTINCT user_id) as cnt FROM event_clicks"
    ).fetchone()["cnt"]

    # Top clickers
    top = conn.execute(
        "SELECT user_id, username, COUNT(*) as cnt FROM event_clicks "
        "GROUP BY user_id ORDER BY cnt DESC LIMIT 10"
    ).fetchall()
    conn.close()

    text = (
        f"📊 <b>THỐNG KÊ EVENT CLICK</b>\n\n"
        f"📈 Tổng click: <b>{total}</b>\n"
        f"📅 Hôm nay: <b>{today_cnt}</b>\n"
        f"👥 Unique users: <b>{unique}</b>\n\n"
        f"<b>Top clickers:</b>\n"
    )
    for i, r in enumerate(top, 1):
        name = r["username"] or f"User#{r['user_id']}"
        text += f"{i}. @{name} - {r['cnt']} clicks\n"

    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

async def cmd_checkin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: /checkin_stats - Thống kê check-in."""
    if not is_admin(update.effective_user):
        await update.message.reply_text("⛔ Bạn không có quyền sử dụng lệnh này.")
        return

    conn = get_db()
    total_users = conn.execute("SELECT COUNT(*) as cnt FROM users").fetchone()["cnt"]
    total_checkins = conn.execute("SELECT COUNT(*) as cnt FROM checkins").fetchone()["cnt"]
    today_checkins = conn.execute(
        "SELECT COUNT(*) as cnt FROM checkins WHERE date = ?", (vn_today(),)
    ).fetchone()["cnt"]
    avg_streak = conn.execute(
        "SELECT AVG(streak) as avg FROM users WHERE streak > 0"
    ).fetchone()["avg"] or 0
    max_streak = conn.execute(
        "SELECT MAX(streak) as mx FROM users"
    ).fetchone()["mx"] or 0
    total_points = conn.execute(
        "SELECT SUM(points) as total FROM users"
    ).fetchone()["total"] or 0
    conn.close()

    text = (
        f"📊 <b>THỐNG KÊ CHECK-IN</b>\n\n"
        f"👥 Tổng users: <b>{total_users}</b>\n"
        f"📅 Tổng check-in: <b>{total_checkins}</b>\n"
        f"📅 Hôm nay: <b>{today_checkins}</b>\n"
        f"🔥 Streak TB: <b>{avg_streak:.1f} ngày</b>\n"
        f"🔥 Streak cao nhất: <b>{max_streak} ngày</b>\n"
        f"💎 Tổng điểm phát: <b>{total_points}</b>"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

async def cmd_referral_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: /referral_stats - Thống kê referral."""
    if not is_admin(update.effective_user):
        await update.message.reply_text("⛔ Bạn không có quyền sử dụng lệnh này.")
        return

    conn = get_db()
    total = conn.execute("SELECT COUNT(*) as cnt FROM referrals").fetchone()["cnt"]
    qualified = conn.execute(
        "SELECT COUNT(*) as cnt FROM referrals WHERE checkin_count >= 3"
    ).fetchone()["cnt"]
    rewarded = conn.execute(
        "SELECT COUNT(*) as cnt FROM referrals WHERE rewarded = 1"
    ).fetchone()["cnt"]

    # Top referrers
    top = conn.execute(
        "SELECT r.referrer_id, u.username, u.full_name, COUNT(*) as cnt "
        "FROM referrals r LEFT JOIN users u ON r.referrer_id = u.user_id "
        "GROUP BY r.referrer_id ORDER BY cnt DESC LIMIT 10"
    ).fetchall()
    conn.close()

    text = (
        f"📊 <b>THỐNG KÊ REFERRAL</b>\n\n"
        f"👥 Tổng referral: <b>{total}</b>\n"
        f"✅ Đủ 3 ngày check-in: <b>{qualified}</b>\n"
        f"💰 Đã thưởng: <b>{rewarded}</b>\n\n"
        f"<b>Top người mời:</b>\n"
    )
    for i, r in enumerate(top, 1):
        name = r["full_name"] or r["username"] or f"User#{r['referrer_id']}"
        text += f"{i}. {name} - {r['cnt']} người\n"

    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

async def cmd_export_checkin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: /export_checkin - Xuất CSV check-in."""
    if not is_admin(update.effective_user):
        await update.message.reply_text("⛔ Bạn không có quyền sử dụng lệnh này.")
        return

    conn = get_db()
    rows = conn.execute(
        "SELECT c.user_id, u.username, u.full_name, c.date, c.points, c.streak "
        "FROM checkins c LEFT JOIN users u ON c.user_id = u.user_id "
        "ORDER BY c.date DESC"
    ).fetchall()
    conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["user_id", "username", "full_name", "date", "points", "streak"])
    for r in rows:
        writer.writerow([r["user_id"], r["username"], r["full_name"],
                         r["date"], r["points"], r["streak"]])

    output.seek(0)
    buf = io.BytesIO(output.getvalue().encode("utf-8-sig"))
    buf.name = f"checkin_{vn_today()}.csv"

    await update.message.reply_document(
        document=buf,
        caption=f"📊 Xuất check-in data - {vn_today()}"
    )

async def cmd_export_referral(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: /export_referral - Xuất CSV referral."""
    if not is_admin(update.effective_user):
        await update.message.reply_text("⛔ Bạn không có quyền sử dụng lệnh này.")
        return

    conn = get_db()
    rows = conn.execute(
        "SELECT r.referrer_id, u1.username as referrer_name, "
        "r.referred_id, u2.username as referred_name, "
        "r.checkin_count, r.rewarded, r.created_at "
        "FROM referrals r "
        "LEFT JOIN users u1 ON r.referrer_id = u1.user_id "
        "LEFT JOIN users u2 ON r.referred_id = u2.user_id "
        "ORDER BY r.created_at DESC"
    ).fetchall()
    conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["referrer_id", "referrer_name", "referred_id",
                     "referred_name", "checkin_count", "rewarded", "created_at"])
    for r in rows:
        writer.writerow([r["referrer_id"], r["referrer_name"], r["referred_id"],
                         r["referred_name"], r["checkin_count"],
                         r["rewarded"], r["created_at"]])

    output.seek(0)
    buf = io.BytesIO(output.getvalue().encode("utf-8-sig"))
    buf.name = f"referral_{vn_today()}.csv"

    await update.message.reply_document(
        document=buf,
        caption=f"📊 Xuất referral data - {vn_today()}"
    )

async def cmd_payment_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: /payment_report - Báo cáo thanh toán USDT."""
    if not is_admin(update.effective_user):
        await update.message.reply_text("⛔ Bạn không có quyền sử dụng lệnh này.")
        return

    conn = get_db()
    total_usdt = conn.execute(
        "SELECT COALESCE(SUM(usdt), 0) as total FROM redemptions"
    ).fetchone()["total"]
    total_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM redemptions"
    ).fetchone()["cnt"]
    pending = conn.execute(
        "SELECT r.user_id, u.username, u.full_name, SUM(r.usdt) as total_usdt, "
        "COUNT(*) as cnt FROM redemptions r "
        "LEFT JOIN users u ON r.user_id = u.user_id "
        "GROUP BY r.user_id ORDER BY total_usdt DESC"
    ).fetchall()
    conn.close()

    text = (
        f"💵 <b>BÁO CÁO THANH TOÁN USDT</b>\n\n"
        f"📊 Tổng giao dịch: <b>{total_count}</b>\n"
        f"💰 Tổng USDT: <b>{total_usdt:.2f}</b>\n\n"
        f"<b>Chi tiết theo user:</b>\n"
    )
    for i, r in enumerate(pending[:20], 1):
        name = r["full_name"] or r["username"] or f"User#{r['user_id']}"
        text += f"{i}. {name} - {r['total_usdt']:.2f} USDT ({r['cnt']} lần)\n"

    if not pending:
        text += "Chưa có giao dịch nào.\n"

    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: /users - Danh sách user."""
    if not is_admin(update.effective_user):
        await update.message.reply_text("⛔ Bạn không có quyền sử dụng lệnh này.")
        return

    conn = get_db()
    users = conn.execute(
        "SELECT * FROM users ORDER BY points DESC LIMIT 50"
    ).fetchall()
    total = conn.execute("SELECT COUNT(*) as cnt FROM users").fetchone()["cnt"]
    conn.close()

    text = f"👥 <b>DANH SÁCH USER</b> (Top 50/{total})\n\n"
    for i, u in enumerate(users, 1):
        name = u["full_name"] or u["username"] or f"User#{u['user_id']}"
        text += (
            f"{i}. {name} | {u['points']}đ | "
            f"🔥{u['streak']} | ID: {u['user_id']}\n"
        )

    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: /broadcast Nội dung - Gửi tin push cho tất cả user."""
    if not is_admin(update.effective_user):
        await update.message.reply_text("⛔ Bạn không có quyền sử dụng lệnh này.")
        return

    if not context.args:
        await update.message.reply_text(
            "📌 Cách dùng: /broadcast Nội dung tin nhắn"
        )
        return

    content = " ".join(context.args)
    user_ids = get_all_users()
    success, fail = 0, 0

    for uid in user_ids:
        try:
            await context.bot.send_message(
                chat_id=uid,
                text=f"📢 <b>THÔNG BÁO</b>\n\n{content}",
                parse_mode=ParseMode.HTML
            )
            success += 1
        except Exception as e:
            fail += 1
            logger.warning(f"Broadcast fail to {uid}: {e}")

    await update.message.reply_text(
        f"📢 Broadcast hoàn tất!\n✅ Thành công: {success}\n❌ Thất bại: {fail}"
    )

# ============================================================
# SCHEDULED JOBS (Tự động gửi vào group)
# ============================================================

async def job_send_event(context: ContextTypes.DEFAULT_TYPE):
    """Job: Gửi event link vào tất cả group."""
    groups = get_all_groups()
    if not groups:
        logger.info("Không có group nào để gửi event.")
        return

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎁 Tham gia Event Tovest", callback_data="event_click")]
    ])

    now = vn_now().strftime("%H:%M")
    for chat_id in groups:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"🎉 <b>EVENT TOVEST - {now}</b>\n\n"
                    f"Bấm nút bên dưới để tham gia event và nhận thưởng!\n"
                    f"⏰ Khung giờ: 08:00 | 12:00 | 18:00 | 21:00"
                ),
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard
            )
            logger.info(f"Đã gửi event link vào group {chat_id}")
        except Exception as e:
            logger.error(f"Gửi event thất bại cho group {chat_id}: {e}")

async def job_checkin_reminder(context: ContextTypes.DEFAULT_TYPE):
    """Job: Nhắc nhở check-in vào group."""
    groups = get_all_groups()
    if not groups:
        return

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Check-in ngay!", callback_data="checkin")],
        [InlineKeyboardButton("📋 Quy tắc", callback_data="rules")]
    ])

    conn = get_db()
    today_cnt = conn.execute(
        "SELECT COUNT(*) as cnt FROM checkins WHERE date = ?", (vn_today(),)
    ).fetchone()["cnt"]
    conn.close()

    for chat_id in groups:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"⏰ <b>NHẮC NHỞ CHECK-IN!</b>\n\n"
                    f"Đừng quên check-in hôm nay để giữ streak!\n"
                    f"📊 Đã có <b>{today_cnt}</b> người check-in hôm nay.\n\n"
                    f"💡 Check-in liên tục để nhận bonus lớn!"
                ),
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard
            )
        except Exception as e:
            logger.error(f"Gửi nhắc nhở thất bại cho group {chat_id}: {e}")

async def job_weekly_report(context: ContextTypes.DEFAULT_TYPE):
    """Job: Báo cáo hàng tuần (thứ 2, 9:00 VN)."""
    groups = get_all_groups()
    if not groups:
        return

    conn = get_db()
    # Top 10 tuần này
    top = conn.execute(
        "SELECT u.user_id, u.username, u.full_name, u.points, u.streak "
        "FROM users u ORDER BY u.points DESC LIMIT 10"
    ).fetchall()

    # Thống kê tuần
    total_users = conn.execute("SELECT COUNT(*) as cnt FROM users").fetchone()["cnt"]
    week_checkins = conn.execute(
        "SELECT COUNT(*) as cnt FROM checkins "
        "WHERE date >= date('now', '-7 days')"
    ).fetchone()["cnt"]

    # Thanh toán pending
    pending_usdt = conn.execute(
        "SELECT COALESCE(SUM(usdt), 0) as total FROM redemptions"
    ).fetchone()["total"]
    conn.close()

    medals = ["🥇", "🥈", "🥉"]
    text = (
        f"📊 <b>BÁO CÁO HÀNG TUẦN</b>\n"
        f"📅 {vn_today()}\n\n"
        f"👥 Tổng users: <b>{total_users}</b>\n"
        f"📅 Check-in 7 ngày qua: <b>{week_checkins}</b>\n"
        f"💵 Tổng USDT đã quy đổi: <b>{pending_usdt:.2f}</b>\n\n"
        f"🏆 <b>TOP 10 BẢNG XẾP HẠNG:</b>\n"
    )
    for i, u in enumerate(top):
        medal = medals[i] if i < 3 else f"{i+1}."
        name = u["full_name"] or u["username"] or f"User#{u['user_id']}"
        text += f"{medal} {name} - <b>{u['points']}đ</b> (🔥{u['streak']})\n"

    text += (
        f"\n💵 <b>THÔNG BÁO THANH TOÁN:</b>\n"
        f"Các yêu cầu quy đổi USDT sẽ được xử lý trong tuần này.\n"
        f"Vui lòng kiểm tra tài khoản Tovest."
    )

    for chat_id in groups:
        try:
            await context.bot.send_message(
                chat_id=chat_id, text=text, parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Gửi báo cáo tuần thất bại cho group {chat_id}: {e}")

# ============================================================
# AUTO-DETECT GROUP
# ============================================================

async def on_new_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Tự động lưu group khi bot được thêm vào hoặc có tin nhắn."""
    chat = update.effective_chat
    if chat and chat.type in ("group", "supergroup"):
        save_group(chat.id, chat.title or "")

# ============================================================
# ERROR HANDLER
# ============================================================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý lỗi toàn cục."""
    logger.error(f"Exception: {context.error}", exc_info=context.error)

# ============================================================
# SETUP SCHEDULED JOBS
# ============================================================

def setup_jobs(app: Application):
    """Thiết lập tất cả scheduled jobs."""
    jq = app.job_queue

    # Event link: 08:00, 12:00, 18:00, 21:00 VN
    for hour in [8, 12, 18, 21]:
        jq.run_daily(
            job_send_event,
            time=time(hour=hour, minute=0, tzinfo=VN_TZ),
            name=f"event_{hour:02d}00"
        )
        logger.info(f"Scheduled: Event link lúc {hour:02d}:00 VN")

    # Check-in reminder: 08:00, 12:00, 20:00 VN
    for hour in [8, 12, 20]:
        jq.run_daily(
            job_checkin_reminder,
            time=time(hour=hour, minute=0, tzinfo=VN_TZ),
            name=f"checkin_reminder_{hour:02d}00"
        )
        logger.info(f"Scheduled: Check-in reminder lúc {hour:02d}:00 VN")

    # Báo cáo hàng tuần: Thứ 2, 09:00 VN (days=(0,) = Monday)
    jq.run_daily(
        job_weekly_report,
        time=time(hour=9, minute=0, tzinfo=VN_TZ),
        days=(0,),  # 0 = Monday
        name="weekly_report"
    )
    logger.info("Scheduled: Báo cáo hàng tuần - Thứ 2 lúc 09:00 VN")

# ============================================================
# SETUP BOT COMMANDS MENU
# ============================================================

async def post_init(app: Application):
    """Thiết lập menu commands sau khi bot khởi động."""
    commands = [
        BotCommand("start", "Đăng ký / Bắt đầu"),
        BotCommand("checkin", "Check-in hàng ngày"),
        BotCommand("myinfo", "Thông tin cá nhân"),
        BotCommand("myreferral", "Link mời bạn bè"),
        BotCommand("referral_info", "Thống kê mời bạn bè"),
        BotCommand("leaderboard", "Bảng xếp hạng"),
        BotCommand("rules", "Quy tắc chương trình"),
        BotCommand("event", "Xem event link"),
    ]
    await app.bot.set_my_commands(commands)
    logger.info("Bot commands menu đã được thiết lập.")

# ============================================================
# MAIN
# ============================================================

def main():
    """Khởi chạy bot."""
    # Khởi tạo database
    init_db()

    # Tạo Application
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    # --- Đăng ký handlers ---

    # User commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("checkin", cmd_checkin))
    app.add_handler(CommandHandler("myinfo", cmd_myinfo))
    app.add_handler(CommandHandler("myreferral", cmd_myreferral))
    app.add_handler(CommandHandler("referral_info", cmd_referral_info))
    app.add_handler(CommandHandler("leaderboard", cmd_leaderboard))
    app.add_handler(CommandHandler("rules", cmd_rules))
    app.add_handler(CommandHandler("event", cmd_event))

    # Admin commands
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("checkin_stats", cmd_checkin_stats))
    app.add_handler(CommandHandler("referral_stats", cmd_referral_stats))
    app.add_handler(CommandHandler("export_checkin", cmd_export_checkin))
    app.add_handler(CommandHandler("export_referral", cmd_export_referral))
    app.add_handler(CommandHandler("payment_report", cmd_payment_report))
    app.add_handler(CommandHandler("users", cmd_users))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))

    # Callback handlers (inline buttons)
    app.add_handler(CallbackQueryHandler(callback_checkin, pattern="^checkin$"))
    app.add_handler(CallbackQueryHandler(callback_rules, pattern="^rules$"))
    app.add_handler(CallbackQueryHandler(callback_event_click, pattern="^event_click$"))
    app.add_handler(CallbackQueryHandler(callback_redeem, pattern="^redeem$"))

    # Auto-detect group (bắt mọi message trong group để lưu chat_id)
    app.add_handler(MessageHandler(
        filters.ChatType.GROUPS & ~filters.COMMAND,
        on_new_chat
    ), group=1)

    # Error handler
    app.add_error_handler(error_handler)

    # Setup scheduled jobs
    setup_jobs(app)

    # Chạy bot bằng polling
    logger.info("🚀 Bot đang khởi động...")
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True
    )

if __name__ == "__main__":
    main()
