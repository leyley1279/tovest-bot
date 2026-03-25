#!/usr/bin/env python3
"""
Tovest Telegram Bot - Event & Check-in & Referral System (Multilingual)
========================================================================
Bot quản lý event link, check-in hàng ngày, referral, quy đổi USDT.
Hỗ trợ đa ngôn ngữ: Tiếng Việt (vi), Tiếng Anh (en), Tiếng Indonesia (id).
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
# Danh sách tất cả admin
ADMIN_USERNAMES = ["leyleyeyy", "imlipark", "maichan004", "bymnguyen", "iouisaga"]
EVENT_LINK = "https://tovest.com/en-US?m=globalbio&c=1600000707&ext=1"

# URLs cho inline buttons trong bài post
POST_URL_OPEN_ACCOUNT = "https://tovest.com/en-US?m=globalbio&c=1600000707&ext=1"
POST_URL_JOIN_COMMUNITY = "https://t.me/+gp8TVKCz461lZDE1"
POST_URL_CONTACT_ADMIN = "https://t.me/leyleyeyy"
POST_URL_DEPOSIT = "https://tovest.com/en-US?m=globalbio&c=1600000707&ext=1"

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
DB_PATH = os.getenv("DB_PATH", "bot_data.db")
DEFAULT_LANG = "vi"  # Ngôn ngữ mặc định

# Điểm check-in
BASE_POINTS = 10
STREAK_BONUS = 5  # +5 mỗi ngày streak
MILESTONE_BONUSES = {7: 50, 14: 120, 30: 300, 60: 700, 100: 1500}

# Quy đổi USDT
POINTS_PER_REDEEM = 500
USDT_PER_REDEEM = 0.05

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("ToveBot")

# ============================================================
# HỆ THỐNG ĐA NGÔN NGỮ (i18n)
# ============================================================

LANG = {
    # -------------------------------------------------------
    # TIẾNG VIỆT
    # -------------------------------------------------------
    "vi": {
        # --- Nút bấm (Inline Keyboard) ---
        "btn_checkin": "✅ Check-in ngay!",
        "btn_rules": "📋 Quy tắc",
        "btn_join_event": "🎁 Tham gia Event Tovest",
        "btn_redeem": "💵 Quy đổi {usdt} USDT ({points}đ)",

        # --- /start ---
        "start_welcome": (
            "👋 Chào <b>{name}</b>!\n\n"
            "🤖 Chào mừng đến với <b>Tovest Bot</b>!\n\n"
            "📌 Các lệnh chính:\n"
            "/checkin - Check-in nhận điểm\n"
            "/myinfo - Thông tin cá nhân\n"
            "/myreferral - Link mời bạn bè\n"
            "/leaderboard - Bảng xếp hạng\n"
            "/rules - Quy tắc chương trình\n"
            "/event - Xem event link\n\n"
            "💡 Hãy check-in mỗi ngày để tích điểm!"
        ),

        # --- /checkin ---
        "checkin_prompt": (
            "📅 <b>DAILY CHECK-IN</b>\n\n"
            "Bấm nút bên dưới để check-in hôm nay!\n"
            "🎯 +10 điểm cơ bản + streak bonus"
        ),
        "checkin_already": "⚠️ Bạn đã check-in hôm nay rồi!",
        "checkin_success": "✅ <b>{name}</b> đã check-in thành công!",
        "checkin_streak": "📊 Streak: <b>{streak} ngày</b> 🔥",
        "checkin_points": "💰 Điểm nhận: <b>+{points}</b>",
        "checkin_milestone": "🎉 Milestone bonus ({streak} ngày): <b>+{bonus}</b>",
        "checkin_total_today": "📈 Tổng hôm nay: <b>+{total}</b>",
        "checkin_total_points": "💎 Tổng điểm: <b>{points}</b>",

        # --- /rules ---
        "rules_text": (
            "📋 <b>QUY TẮC CHƯƠNG TRÌNH TOVEST BOT</b>\n\n"
            "<b>1. Check-in hàng ngày:</b>\n"
            "• Mỗi ngày bấm nút ✅ Check-in để nhận điểm\n"
            "• Mỗi user chỉ check-in được 1 lần/ngày\n"
            "• Điểm cơ bản: <b>+10 điểm</b>\n"
            "• Streak bonus: <b>+5 điểm</b> cho mỗi ngày liên tiếp\n"
            "• Ví dụ: Ngày 1 = 10đ, Ngày 2 = 15đ, Ngày 3 = 20đ...\n\n"
            "<b>2. Milestone bonus (thưởng cột mốc):</b>\n"
            "• 7 ngày liên tiếp: <b>+50 điểm</b>\n"
            "• 14 ngày liên tiếp: <b>+120 điểm</b>\n"
            "• 30 ngày liên tiếp: <b>+300 điểm</b>\n"
            "• 60 ngày liên tiếp: <b>+700 điểm</b>\n"
            "• 100 ngày liên tiếp: <b>+1500 điểm</b>\n\n"
            "<b>3. Mời bạn bè (Referral):</b>\n"
            "• Chia sẻ link mời: t.me/{bot}?start=ref_USERID\n"
            "• Bạn bè join group + check-in 3 ngày → bạn nhận <b>+10 điểm</b>\n\n"
            "<b>4. Quy đổi USDT:</b>\n"
            "• <b>500 điểm = 0.05 USDT</b>\n"
            "• Trả vào tài khoản Tovest\n"
            "• Thanh toán được xử lý mỗi thứ 2 hàng tuần\n\n"
            "<b>5. Event link:</b>\n"
            "• Bot tự động gửi link event vào 08:00, 12:00, 18:00, 21:00\n"
            "• Bấm vào link để tham gia event trên Tovest\n\n"
            "<b>6. Bảng xếp hạng:</b>\n"
            "• Top user theo tổng điểm và streak dài nhất\n"
            "• Cập nhật realtime qua lệnh /leaderboard"
        ),

        # --- /myinfo ---
        "myinfo_title": "👤 <b>Thông tin của {name}</b>\n",
        "myinfo_points": "💎 Tổng điểm: <b>{points}</b>",
        "myinfo_streak": "🔥 Streak hiện tại: <b>{streak} ngày</b>",
        "myinfo_checkins": "📅 Tổng ngày check-in: <b>{count}</b>",
        "myinfo_referrals": "👥 Đã mời: <b>{count} người</b>",
        "myinfo_redeemed": "💵 Đã quy đổi: <b>{usdt:.2f} USDT</b>",
        "myinfo_can_redeem": "🔄 Có thể quy đổi: <b>{count} lần</b> ({usdt:.2f} USDT)",
        "myinfo_last_checkin": "📌 Check-in cuối: {date}",
        "myinfo_no_checkin": "Chưa check-in",

        # --- Quy đổi USDT ---
        "redeem_not_enough": "⚠️ Bạn cần ít nhất {points} điểm để quy đổi!",
        "redeem_success": (
            "💵 <b>Quy đổi thành công!</b>\n\n"
            "• Đã trừ: <b>{points} điểm</b>\n"
            "• Nhận: <b>{usdt} USDT</b>\n"
            "• Điểm còn lại: <b>{remaining}</b>\n\n"
            "📌 USDT sẽ được trả vào tài khoản Tovest.\n"
            "Thanh toán xử lý mỗi thứ 2 hàng tuần."
        ),

        # --- /myreferral ---
        "referral_link": (
            "👥 <b>Link mời bạn bè của bạn:</b>\n\n"
            "🔗 <code>{link}</code>\n\n"
            "📌 Chia sẻ link này cho bạn bè.\n"
            "Khi bạn bè join + check-in 3 ngày → bạn nhận <b>+10 điểm</b>!"
        ),

        # --- /referral_info ---
        "referral_empty": "📭 Bạn chưa mời ai. Dùng /myreferral để lấy link mời!",
        "referral_stats_title": "👥 <b>Thống kê mời bạn bè</b>\n",
        "referral_stats_total": "📊 Tổng đã mời: <b>{count}</b>",
        "referral_stats_qualified": "✅ Đã đủ 3 ngày check-in: <b>{count}</b>",
        "referral_stats_points": "💰 Điểm từ referral: <b>{points}</b>",
        "referral_stats_detail": "<b>Chi tiết:</b>",
        "referral_stats_more": "... và {count} người khác",

        # --- /event ---
        "event_title": (
            "🎉 <b>EVENT TOVEST</b>\n\n"
            "Bấm nút bên dưới để tham gia event và nhận thưởng!"
        ),
        "event_click_alert": "🔗 Đang mở link event...",
        "event_click_msg": (
            "🎁 <b>{name}</b>, đây là link event:\n\n"
            "👉 <a href=\"{link}\">Tham gia Event Tovest</a>\n\n"
            "📌 Mở link trên trình duyệt để tham gia!"
        ),

        # --- /leaderboard ---
        "lb_title": "🏆 <b>BẢNG XẾP HẠNG</b>\n",
        "lb_top_points": "<b>💎 Top Điểm:</b>",
        "lb_top_streak": "<b>🔥 Top Streak:</b>",
        "lb_no_data": "Chưa có dữ liệu.",
        "lb_points_fmt": "{medal} {name} - <b>{points}đ</b> (🔥{streak})",
        "lb_streak_fmt": "{medal} {name} - <b>{streak} ngày</b> ({points}đ)",

        # --- Admin: /stats ---
        "admin_no_perm": "⛔ Bạn không có quyền sử dụng lệnh này.",
        "stats_title": "📊 <b>THỐNG KÊ EVENT CLICK</b>\n",
        "stats_total": "📈 Tổng click: <b>{count}</b>",
        "stats_today": "📅 Hôm nay: <b>{count}</b>",
        "stats_unique": "👥 Unique users: <b>{count}</b>",
        "stats_top_clickers": "<b>Top clickers:</b>",

        # --- Admin: /checkin_stats ---
        "cstats_title": "📊 <b>THỐNG KÊ CHECK-IN</b>\n",
        "cstats_users": "👥 Tổng users: <b>{count}</b>",
        "cstats_total": "📅 Tổng check-in: <b>{count}</b>",
        "cstats_today": "📅 Hôm nay: <b>{count}</b>",
        "cstats_avg_streak": "🔥 Streak TB: <b>{avg:.1f} ngày</b>",
        "cstats_max_streak": "🔥 Streak cao nhất: <b>{max} ngày</b>",
        "cstats_total_points": "💎 Tổng điểm phát: <b>{points}</b>",

        # --- Admin: /referral_stats ---
        "rstats_title": "📊 <b>THỐNG KÊ REFERRAL</b>\n",
        "rstats_total": "👥 Tổng referral: <b>{count}</b>",
        "rstats_qualified": "✅ Đủ 3 ngày check-in: <b>{count}</b>",
        "rstats_rewarded": "💰 Đã thưởng: <b>{count}</b>",
        "rstats_top_referrers": "<b>Top người mời:</b>",

        # --- Admin: /payment_report ---
        "pay_title": "💵 <b>BÁO CÁO THANH TOÁN USDT</b>\n",
        "pay_total_tx": "📊 Tổng giao dịch: <b>{count}</b>",
        "pay_total_usdt": "💰 Tổng USDT: <b>{usdt:.2f}</b>",
        "pay_detail": "<b>Chi tiết theo user:</b>",
        "pay_no_tx": "Chưa có giao dịch nào.",

        # --- Admin: /users ---
        "users_title": "👥 <b>DANH SÁCH USER</b> (Top 50/{total})\n",

        # --- Admin: /broadcast ---
        "broadcast_usage": "📌 Cách dùng: /broadcast Nội dung tin nhắn",
        "broadcast_header": "📢 <b>THÔNG BÁO</b>\n\n{content}",
        "broadcast_done": "📢 Broadcast hoàn tất!\n✅ Thành công: {success}\n❌ Thất bại: {fail}",

        # --- Admin: /export ---
        "export_checkin_caption": "📊 Xuất check-in data - {date}",
        "export_referral_caption": "📊 Xuất referral data - {date}",

        # --- Scheduled jobs ---
        "job_event": (
            "🎉 <b>EVENT TOVEST - {time}</b>\n\n"
            "Bấm nút bên dưới để tham gia event và nhận thưởng!\n"
            "⏰ Khung giờ: 08:00 | 12:00 | 18:00 | 21:00"
        ),
        "job_reminder": (
            "⏰ <b>NHẮC NHỞ CHECK-IN!</b>\n\n"
            "Đừng quên check-in hôm nay để giữ streak!\n"
            "📊 Đã có <b>{count}</b> người check-in hôm nay.\n\n"
            "💡 Check-in liên tục để nhận bonus lớn!"
        ),
        "job_weekly_title": "📊 <b>BÁO CÁO HÀNG TUẦN</b>",
        "job_weekly_date": "📅 {date}",
        "job_weekly_users": "👥 Tổng users: <b>{count}</b>",
        "job_weekly_checkins": "📅 Check-in 7 ngày qua: <b>{count}</b>",
        "job_weekly_usdt": "💵 Tổng USDT đã quy đổi: <b>{usdt:.2f}</b>",
        "job_weekly_top": "🏆 <b>TOP 10 BẢNG XẾP HẠNG:</b>",
        "job_weekly_payment": (
            "💵 <b>THÔNG BÁO THANH TOÁN:</b>\n"
            "Các yêu cầu quy đổi USDT sẽ được xử lý trong tuần này.\n"
            "Vui lòng kiểm tra tài khoản Tovest."
        ),

        # --- /setlang ---
        "setlang_usage": (
            "📌 Cách dùng: /setlang vi | en | id\n"
            "• vi - Tiếng Việt\n"
            "• en - English\n"
            "• id - Bahasa Indonesia"
        ),
        "setlang_invalid": "⚠️ Ngôn ngữ không hợp lệ. Chọn: vi, en, id",
        "setlang_success": "✅ Đã đặt ngôn ngữ: <b>{lang_name}</b>",
        "lang_name_vi": "Tiếng Việt 🇻🇳",
        "lang_name_en": "English 🇬🇧",
        "lang_name_id": "Bahasa Indonesia 🇮🇩",

        # --- Post bài vào group ---
        "post_btn_open_account": "📋 Mở tài khoản",
        "post_btn_join_community": "👥 Join cộng đồng",
        "post_btn_contact_admin": "📞 Liên hệ admin",
        "post_btn_deposit": "💰 Nạp ngay",
        "post_usage": (
            "📌 Cách dùng:\n"
            "/post\n"
            "Nội dung bài viết\n"
            "---\n"
            "account: link mở tài khoản\n"
            "community: link join cộng đồng\n"
            "admin: link liên hệ admin\n"
            "deposit: link nạp tiền\n\n"
            "💡 Nếu không có --- thì dùng link mặc định."
        ),
        "post_success": "✅ Đã đăng bài vào <b>{success}</b> group!\n❌ Thất bại: <b>{fail}</b>",
        "post_no_groups": "⚠️ Chưa có group nào đăng ký.",
        "schedule_post_usage": (
            "📌 Cách dùng:\n"
            "/schedule_post HH:MM\n"
            "Nội dung bài viết\n"
            "---\n"
            "account: link\n"
            "community: link\n"
            "admin: link\n"
            "deposit: link"
        ),
        "schedule_post_invalid_time": (
            "⚠️ Định dạng giờ không hợp lệ. Dùng HH:MM (giờ VN).\n"
            "Ví dụ: /schedule_post 14:30 Nội dung bài"
        ),
        "schedule_post_success": (
            "✅ Đã hẹn giờ đăng bài!\n"
            "🆔 ID: <b>{post_id}</b>\n"
            "⏰ Thời gian: <b>{time}</b> (giờ VN)\n"
            "📝 Nội dung: {content}"
        ),
        "scheduled_posts_title": "📋 <b>DANH SÁCH BÀI HẸN GIỜ</b>\n",
        "scheduled_posts_empty": "📭 Không có bài hẹn giờ nào.",
        "scheduled_posts_item": "🆔 <b>{post_id}</b> | ⏰ {time} | 📝 {content}",
        "cancel_post_usage": "📌 Cách dùng: /cancel_post ID",
        "cancel_post_not_found": "⚠️ Không tìm thấy bài hẹn giờ với ID: {post_id}",
        "cancel_post_success": "✅ Đã hủy bài hẹn giờ ID: <b>{post_id}</b>",
        "scheduled_post_sent": "📢 Bài hẹn giờ ID {post_id} đã gửi: ✅ {success} group, ❌ {fail} thất bại.",

        # --- Private reply ---
        "private_reply_sent": "✅ Thông tin đã được gửi qua tin nhắn riêng.",
        "private_reply_error": "⚠️ Không thể gửi tin nhắn riêng. Vui lòng /start bot @{bot_username} trước.",
    },

    # -------------------------------------------------------
    # TIẾNG ANH
    # -------------------------------------------------------
    "en": {
        # --- Nút bấm ---
        "btn_checkin": "✅ Check-in now!",
        "btn_rules": "📋 Rules",
        "btn_join_event": "🎁 Join Tovest Event",
        "btn_redeem": "💵 Redeem {usdt} USDT ({points}pts)",

        # --- /start ---
        "start_welcome": (
            "👋 Hello <b>{name}</b>!\n\n"
            "🤖 Welcome to <b>Tovest Bot</b>!\n\n"
            "📌 Main commands:\n"
            "/checkin - Daily check-in for points\n"
            "/myinfo - Personal info\n"
            "/myreferral - Referral link\n"
            "/leaderboard - Leaderboard\n"
            "/rules - Program rules\n"
            "/event - View event link\n\n"
            "💡 Check in every day to earn points!"
        ),

        # --- /checkin ---
        "checkin_prompt": (
            "📅 <b>DAILY CHECK-IN</b>\n\n"
            "Press the button below to check in today!\n"
            "🎯 +10 base points + streak bonus"
        ),
        "checkin_already": "⚠️ You have already checked in today!",
        "checkin_success": "✅ <b>{name}</b> checked in successfully!",
        "checkin_streak": "📊 Streak: <b>{streak} days</b> 🔥",
        "checkin_points": "💰 Points earned: <b>+{points}</b>",
        "checkin_milestone": "🎉 Milestone bonus ({streak} days): <b>+{bonus}</b>",
        "checkin_total_today": "📈 Total today: <b>+{total}</b>",
        "checkin_total_points": "💎 Total points: <b>{points}</b>",

        # --- /rules ---
        "rules_text": (
            "📋 <b>TOVEST BOT PROGRAM RULES</b>\n\n"
            "<b>1. Daily Check-in:</b>\n"
            "• Press ✅ Check-in button every day to earn points\n"
            "• Each user can only check in once per day\n"
            "• Base points: <b>+10 points</b>\n"
            "• Streak bonus: <b>+5 points</b> for each consecutive day\n"
            "• Example: Day 1 = 10pts, Day 2 = 15pts, Day 3 = 20pts...\n\n"
            "<b>2. Milestone bonus:</b>\n"
            "• 7 consecutive days: <b>+50 points</b>\n"
            "• 14 consecutive days: <b>+120 points</b>\n"
            "• 30 consecutive days: <b>+300 points</b>\n"
            "• 60 consecutive days: <b>+700 points</b>\n"
            "• 100 consecutive days: <b>+1500 points</b>\n\n"
            "<b>3. Invite friends (Referral):</b>\n"
            "• Share invite link: t.me/{bot}?start=ref_USERID\n"
            "• Friend joins + checks in 3 days → you get <b>+10 points</b>\n\n"
            "<b>4. Redeem USDT:</b>\n"
            "• <b>500 points = 0.05 USDT</b>\n"
            "• Paid to your Tovest account\n"
            "• Payments processed every Monday\n\n"
            "<b>5. Event link:</b>\n"
            "• Bot sends event links at 08:00, 12:00, 18:00, 21:00\n"
            "• Click the link to join events on Tovest\n\n"
            "<b>6. Leaderboard:</b>\n"
            "• Top users by total points and longest streak\n"
            "• Updated in real-time via /leaderboard"
        ),

        # --- /myinfo ---
        "myinfo_title": "👤 <b>Info of {name}</b>\n",
        "myinfo_points": "💎 Total points: <b>{points}</b>",
        "myinfo_streak": "🔥 Current streak: <b>{streak} days</b>",
        "myinfo_checkins": "📅 Total check-in days: <b>{count}</b>",
        "myinfo_referrals": "👥 Invited: <b>{count} people</b>",
        "myinfo_redeemed": "💵 Redeemed: <b>{usdt:.2f} USDT</b>",
        "myinfo_can_redeem": "🔄 Can redeem: <b>{count} times</b> ({usdt:.2f} USDT)",
        "myinfo_last_checkin": "📌 Last check-in: {date}",
        "myinfo_no_checkin": "Not yet",

        # --- Quy đổi USDT ---
        "redeem_not_enough": "⚠️ You need at least {points} points to redeem!",
        "redeem_success": (
            "💵 <b>Redeem successful!</b>\n\n"
            "• Deducted: <b>{points} points</b>\n"
            "• Received: <b>{usdt} USDT</b>\n"
            "• Remaining points: <b>{remaining}</b>\n\n"
            "📌 USDT will be paid to your Tovest account.\n"
            "Payments processed every Monday."
        ),

        # --- /myreferral ---
        "referral_link": (
            "👥 <b>Your referral link:</b>\n\n"
            "🔗 <code>{link}</code>\n\n"
            "📌 Share this link with friends.\n"
            "When they join + check in 3 days → you get <b>+10 points</b>!"
        ),

        # --- /referral_info ---
        "referral_empty": "📭 You haven't invited anyone yet. Use /myreferral to get your link!",
        "referral_stats_title": "👥 <b>Referral Statistics</b>\n",
        "referral_stats_total": "📊 Total invited: <b>{count}</b>",
        "referral_stats_qualified": "✅ Completed 3-day check-in: <b>{count}</b>",
        "referral_stats_points": "💰 Points from referrals: <b>{points}</b>",
        "referral_stats_detail": "<b>Details:</b>",
        "referral_stats_more": "... and {count} more",

        # --- /event ---
        "event_title": (
            "🎉 <b>TOVEST EVENT</b>\n\n"
            "Press the button below to join the event and earn rewards!"
        ),
        "event_click_alert": "🔗 Opening event link...",
        "event_click_msg": (
            "🎁 <b>{name}</b>, here is the event link:\n\n"
            "👉 <a href=\"{link}\">Join Tovest Event</a>\n\n"
            "📌 Open the link in your browser to participate!"
        ),

        # --- /leaderboard ---
        "lb_title": "🏆 <b>LEADERBOARD</b>\n",
        "lb_top_points": "<b>💎 Top Points:</b>",
        "lb_top_streak": "<b>🔥 Top Streak:</b>",
        "lb_no_data": "No data yet.",
        "lb_points_fmt": "{medal} {name} - <b>{points}pts</b> (🔥{streak})",
        "lb_streak_fmt": "{medal} {name} - <b>{streak} days</b> ({points}pts)",

        # --- Admin: /stats ---
        "admin_no_perm": "⛔ You do not have permission to use this command.",
        "stats_title": "📊 <b>EVENT CLICK STATISTICS</b>\n",
        "stats_total": "📈 Total clicks: <b>{count}</b>",
        "stats_today": "📅 Today: <b>{count}</b>",
        "stats_unique": "👥 Unique users: <b>{count}</b>",
        "stats_top_clickers": "<b>Top clickers:</b>",

        # --- Admin: /checkin_stats ---
        "cstats_title": "📊 <b>CHECK-IN STATISTICS</b>\n",
        "cstats_users": "👥 Total users: <b>{count}</b>",
        "cstats_total": "📅 Total check-ins: <b>{count}</b>",
        "cstats_today": "📅 Today: <b>{count}</b>",
        "cstats_avg_streak": "🔥 Avg streak: <b>{avg:.1f} days</b>",
        "cstats_max_streak": "🔥 Max streak: <b>{max} days</b>",
        "cstats_total_points": "💎 Total points issued: <b>{points}</b>",

        # --- Admin: /referral_stats ---
        "rstats_title": "📊 <b>REFERRAL STATISTICS</b>\n",
        "rstats_total": "👥 Total referrals: <b>{count}</b>",
        "rstats_qualified": "✅ Completed 3-day check-in: <b>{count}</b>",
        "rstats_rewarded": "💰 Rewarded: <b>{count}</b>",
        "rstats_top_referrers": "<b>Top referrers:</b>",

        # --- Admin: /payment_report ---
        "pay_title": "💵 <b>USDT PAYMENT REPORT</b>\n",
        "pay_total_tx": "📊 Total transactions: <b>{count}</b>",
        "pay_total_usdt": "💰 Total USDT: <b>{usdt:.2f}</b>",
        "pay_detail": "<b>Details by user:</b>",
        "pay_no_tx": "No transactions yet.",

        # --- Admin: /users ---
        "users_title": "👥 <b>USER LIST</b> (Top 50/{total})\n",

        # --- Admin: /broadcast ---
        "broadcast_usage": "📌 Usage: /broadcast Message content",
        "broadcast_header": "📢 <b>ANNOUNCEMENT</b>\n\n{content}",
        "broadcast_done": "📢 Broadcast complete!\n✅ Success: {success}\n❌ Failed: {fail}",

        # --- Admin: /export ---
        "export_checkin_caption": "📊 Check-in data export - {date}",
        "export_referral_caption": "📊 Referral data export - {date}",

        # --- Scheduled jobs ---
        "job_event": (
            "🎉 <b>TOVEST EVENT - {time}</b>\n\n"
            "Press the button below to join the event and earn rewards!\n"
            "⏰ Schedule: 08:00 | 12:00 | 18:00 | 21:00"
        ),
        "job_reminder": (
            "⏰ <b>CHECK-IN REMINDER!</b>\n\n"
            "Don't forget to check in today to keep your streak!\n"
            "📊 <b>{count}</b> people have checked in today.\n\n"
            "💡 Check in consistently for big bonuses!"
        ),
        "job_weekly_title": "📊 <b>WEEKLY REPORT</b>",
        "job_weekly_date": "📅 {date}",
        "job_weekly_users": "👥 Total users: <b>{count}</b>",
        "job_weekly_checkins": "📅 Check-ins last 7 days: <b>{count}</b>",
        "job_weekly_usdt": "💵 Total USDT redeemed: <b>{usdt:.2f}</b>",
        "job_weekly_top": "🏆 <b>TOP 10 LEADERBOARD:</b>",
        "job_weekly_payment": (
            "💵 <b>PAYMENT NOTICE:</b>\n"
            "USDT redemption requests will be processed this week.\n"
            "Please check your Tovest account."
        ),

        # --- /setlang ---
        "setlang_usage": (
            "📌 Usage: /setlang vi | en | id\n"
            "• vi - Tiếng Việt\n"
            "• en - English\n"
            "• id - Bahasa Indonesia"
        ),
        "setlang_invalid": "⚠️ Invalid language. Choose: vi, en, id",
        "setlang_success": "✅ Language set to: <b>{lang_name}</b>",
        "lang_name_vi": "Tiếng Việt 🇻🇳",
        "lang_name_en": "English 🇬🇧",
        "lang_name_id": "Bahasa Indonesia 🇮🇩",

        # --- Post bài vào group ---
        "post_btn_open_account": "📋 Open Account",
        "post_btn_join_community": "👥 Join Community",
        "post_btn_contact_admin": "📞 Contact Admin",
        "post_btn_deposit": "💰 Deposit Now",
        "post_usage": (
            "📌 Usage:\n"
            "/post\n"
            "Post content\n"
            "---\n"
            "account: open account link\n"
            "community: join community link\n"
            "admin: contact admin link\n"
            "deposit: deposit link\n\n"
            "💡 If no --- section, default links are used."
        ),
        "post_success": "✅ Posted to <b>{success}</b> groups!\n❌ Failed: <b>{fail}</b>",
        "post_no_groups": "⚠️ No registered groups found.",
        "schedule_post_usage": (
            "📌 Usage:\n"
            "/schedule_post HH:MM\n"
            "Post content\n"
            "---\n"
            "account: link\n"
            "community: link\n"
            "admin: link\n"
            "deposit: link"
        ),
        "schedule_post_invalid_time": (
            "⚠️ Invalid time format. Use HH:MM (Vietnam time).\n"
            "Example: /schedule_post 14:30 Post content"
        ),
        "schedule_post_success": (
            "✅ Post scheduled!\n"
            "🆔 ID: <b>{post_id}</b>\n"
            "⏰ Time: <b>{time}</b> (Vietnam time)\n"
            "📝 Content: {content}"
        ),
        "scheduled_posts_title": "📋 <b>SCHEDULED POSTS</b>\n",
        "scheduled_posts_empty": "📭 No scheduled posts.",
        "scheduled_posts_item": "🆔 <b>{post_id}</b> | ⏰ {time} | 📝 {content}",
        "cancel_post_usage": "📌 Usage: /cancel_post ID",
        "cancel_post_not_found": "⚠️ Scheduled post not found with ID: {post_id}",
        "cancel_post_success": "✅ Cancelled scheduled post ID: <b>{post_id}</b>",
        "scheduled_post_sent": "📢 Scheduled post ID {post_id} sent: ✅ {success} groups, ❌ {fail} failed.",

        # --- Private reply ---
        "private_reply_sent": "✅ Info has been sent to your private chat.",
        "private_reply_error": "⚠️ Cannot send private message. Please /start @{bot_username} first.",
    },

    # -------------------------------------------------------
    # TIẾNG INDONESIA
    # -------------------------------------------------------
    "id": {
        # --- Tombol ---
        "btn_checkin": "✅ Check-in sekarang!",
        "btn_rules": "📋 Aturan",
        "btn_join_event": "🎁 Ikuti Event Tovest",
        "btn_redeem": "💵 Tukar {usdt} USDT ({points}poin)",

        # --- /start ---
        "start_welcome": (
            "👋 Halo <b>{name}</b>!\n\n"
            "🤖 Selamat datang di <b>Tovest Bot</b>!\n\n"
            "📌 Perintah utama:\n"
            "/checkin - Check-in harian untuk poin\n"
            "/myinfo - Info pribadi\n"
            "/myreferral - Link referral\n"
            "/leaderboard - Papan peringkat\n"
            "/rules - Aturan program\n"
            "/event - Lihat link event\n\n"
            "💡 Check-in setiap hari untuk mengumpulkan poin!"
        ),

        # --- /checkin ---
        "checkin_prompt": (
            "📅 <b>CHECK-IN HARIAN</b>\n\n"
            "Tekan tombol di bawah untuk check-in hari ini!\n"
            "🎯 +10 poin dasar + bonus streak"
        ),
        "checkin_already": "⚠️ Anda sudah check-in hari ini!",
        "checkin_success": "✅ <b>{name}</b> berhasil check-in!",
        "checkin_streak": "📊 Streak: <b>{streak} hari</b> 🔥",
        "checkin_points": "💰 Poin diperoleh: <b>+{points}</b>",
        "checkin_milestone": "🎉 Bonus milestone ({streak} hari): <b>+{bonus}</b>",
        "checkin_total_today": "📈 Total hari ini: <b>+{total}</b>",
        "checkin_total_points": "💎 Total poin: <b>{points}</b>",

        # --- /rules ---
        "rules_text": (
            "📋 <b>ATURAN PROGRAM TOVEST BOT</b>\n\n"
            "<b>1. Check-in harian:</b>\n"
            "• Tekan tombol ✅ Check-in setiap hari untuk mendapat poin\n"
            "• Setiap user hanya bisa check-in 1 kali/hari\n"
            "• Poin dasar: <b>+10 poin</b>\n"
            "• Bonus streak: <b>+5 poin</b> untuk setiap hari berturut-turut\n"
            "• Contoh: Hari 1 = 10poin, Hari 2 = 15poin, Hari 3 = 20poin...\n\n"
            "<b>2. Bonus milestone:</b>\n"
            "• 7 hari berturut-turut: <b>+50 poin</b>\n"
            "• 14 hari berturut-turut: <b>+120 poin</b>\n"
            "• 30 hari berturut-turut: <b>+300 poin</b>\n"
            "• 60 hari berturut-turut: <b>+700 poin</b>\n"
            "• 100 hari berturut-turut: <b>+1500 poin</b>\n\n"
            "<b>3. Undang teman (Referral):</b>\n"
            "• Bagikan link undangan: t.me/{bot}?start=ref_USERID\n"
            "• Teman bergabung + check-in 3 hari → Anda dapat <b>+10 poin</b>\n\n"
            "<b>4. Tukar USDT:</b>\n"
            "• <b>500 poin = 0.05 USDT</b>\n"
            "• Dibayar ke akun Tovest Anda\n"
            "• Pembayaran diproses setiap hari Senin\n\n"
            "<b>5. Link event:</b>\n"
            "• Bot mengirim link event pada 08:00, 12:00, 18:00, 21:00\n"
            "• Klik link untuk mengikuti event di Tovest\n\n"
            "<b>6. Papan peringkat:</b>\n"
            "• Top user berdasarkan total poin dan streak terpanjang\n"
            "• Diperbarui realtime melalui /leaderboard"
        ),

        # --- /myinfo ---
        "myinfo_title": "👤 <b>Info {name}</b>\n",
        "myinfo_points": "💎 Total poin: <b>{points}</b>",
        "myinfo_streak": "🔥 Streak saat ini: <b>{streak} hari</b>",
        "myinfo_checkins": "📅 Total hari check-in: <b>{count}</b>",
        "myinfo_referrals": "👥 Telah mengundang: <b>{count} orang</b>",
        "myinfo_redeemed": "💵 Telah ditukar: <b>{usdt:.2f} USDT</b>",
        "myinfo_can_redeem": "🔄 Dapat ditukar: <b>{count} kali</b> ({usdt:.2f} USDT)",
        "myinfo_last_checkin": "📌 Check-in terakhir: {date}",
        "myinfo_no_checkin": "Belum pernah",

        # --- Tukar USDT ---
        "redeem_not_enough": "⚠️ Anda membutuhkan minimal {points} poin untuk menukar!",
        "redeem_success": (
            "💵 <b>Penukaran berhasil!</b>\n\n"
            "• Dikurangi: <b>{points} poin</b>\n"
            "• Diterima: <b>{usdt} USDT</b>\n"
            "• Sisa poin: <b>{remaining}</b>\n\n"
            "📌 USDT akan dibayar ke akun Tovest Anda.\n"
            "Pembayaran diproses setiap hari Senin."
        ),

        # --- /myreferral ---
        "referral_link": (
            "👥 <b>Link referral Anda:</b>\n\n"
            "🔗 <code>{link}</code>\n\n"
            "📌 Bagikan link ini ke teman Anda.\n"
            "Ketika teman bergabung + check-in 3 hari → Anda dapat <b>+10 poin</b>!"
        ),

        # --- /referral_info ---
        "referral_empty": "📭 Anda belum mengundang siapa pun. Gunakan /myreferral untuk mendapatkan link!",
        "referral_stats_title": "👥 <b>Statistik Referral</b>\n",
        "referral_stats_total": "📊 Total diundang: <b>{count}</b>",
        "referral_stats_qualified": "✅ Sudah check-in 3 hari: <b>{count}</b>",
        "referral_stats_points": "💰 Poin dari referral: <b>{points}</b>",
        "referral_stats_detail": "<b>Detail:</b>",
        "referral_stats_more": "... dan {count} orang lagi",

        # --- /event ---
        "event_title": (
            "🎉 <b>EVENT TOVEST</b>\n\n"
            "Tekan tombol di bawah untuk mengikuti event dan mendapat hadiah!"
        ),
        "event_click_alert": "🔗 Membuka link event...",
        "event_click_msg": (
            "🎁 <b>{name}</b>, ini link event-nya:\n\n"
            "👉 <a href=\"{link}\">Ikuti Event Tovest</a>\n\n"
            "📌 Buka link di browser untuk berpartisipasi!"
        ),

        # --- /leaderboard ---
        "lb_title": "🏆 <b>PAPAN PERINGKAT</b>\n",
        "lb_top_points": "<b>💎 Top Poin:</b>",
        "lb_top_streak": "<b>🔥 Top Streak:</b>",
        "lb_no_data": "Belum ada data.",
        "lb_points_fmt": "{medal} {name} - <b>{points}poin</b> (🔥{streak})",
        "lb_streak_fmt": "{medal} {name} - <b>{streak} hari</b> ({points}poin)",

        # --- Admin: /stats ---
        "admin_no_perm": "⛔ Anda tidak memiliki izin untuk menggunakan perintah ini.",
        "stats_title": "📊 <b>STATISTIK KLIK EVENT</b>\n",
        "stats_total": "📈 Total klik: <b>{count}</b>",
        "stats_today": "📅 Hari ini: <b>{count}</b>",
        "stats_unique": "👥 User unik: <b>{count}</b>",
        "stats_top_clickers": "<b>Top clickers:</b>",

        # --- Admin: /checkin_stats ---
        "cstats_title": "📊 <b>STATISTIK CHECK-IN</b>\n",
        "cstats_users": "👥 Total users: <b>{count}</b>",
        "cstats_total": "📅 Total check-in: <b>{count}</b>",
        "cstats_today": "📅 Hari ini: <b>{count}</b>",
        "cstats_avg_streak": "🔥 Rata-rata streak: <b>{avg:.1f} hari</b>",
        "cstats_max_streak": "🔥 Streak tertinggi: <b>{max} hari</b>",
        "cstats_total_points": "💎 Total poin diberikan: <b>{points}</b>",

        # --- Admin: /referral_stats ---
        "rstats_title": "📊 <b>STATISTIK REFERRAL</b>\n",
        "rstats_total": "👥 Total referral: <b>{count}</b>",
        "rstats_qualified": "✅ Sudah check-in 3 hari: <b>{count}</b>",
        "rstats_rewarded": "💰 Sudah diberi hadiah: <b>{count}</b>",
        "rstats_top_referrers": "<b>Top pengundang:</b>",

        # --- Admin: /payment_report ---
        "pay_title": "💵 <b>LAPORAN PEMBAYARAN USDT</b>\n",
        "pay_total_tx": "📊 Total transaksi: <b>{count}</b>",
        "pay_total_usdt": "💰 Total USDT: <b>{usdt:.2f}</b>",
        "pay_detail": "<b>Detail per user:</b>",
        "pay_no_tx": "Belum ada transaksi.",

        # --- Admin: /users ---
        "users_title": "👥 <b>DAFTAR USER</b> (Top 50/{total})\n",

        # --- Admin: /broadcast ---
        "broadcast_usage": "📌 Cara pakai: /broadcast Isi pesan",
        "broadcast_header": "📢 <b>PENGUMUMAN</b>\n\n{content}",
        "broadcast_done": "📢 Broadcast selesai!\n✅ Berhasil: {success}\n❌ Gagal: {fail}",

        # --- Admin: /export ---
        "export_checkin_caption": "📊 Ekspor data check-in - {date}",
        "export_referral_caption": "📊 Ekspor data referral - {date}",

        # --- Scheduled jobs ---
        "job_event": (
            "🎉 <b>EVENT TOVEST - {time}</b>\n\n"
            "Tekan tombol di bawah untuk mengikuti event dan mendapat hadiah!\n"
            "⏰ Jadwal: 08:00 | 12:00 | 18:00 | 21:00"
        ),
        "job_reminder": (
            "⏰ <b>PENGINGAT CHECK-IN!</b>\n\n"
            "Jangan lupa check-in hari ini untuk menjaga streak!\n"
            "📊 <b>{count}</b> orang sudah check-in hari ini.\n\n"
            "💡 Check-in terus-menerus untuk bonus besar!"
        ),
        "job_weekly_title": "📊 <b>LAPORAN MINGGUAN</b>",
        "job_weekly_date": "📅 {date}",
        "job_weekly_users": "👥 Total users: <b>{count}</b>",
        "job_weekly_checkins": "📅 Check-in 7 hari terakhir: <b>{count}</b>",
        "job_weekly_usdt": "💵 Total USDT ditukar: <b>{usdt:.2f}</b>",
        "job_weekly_top": "🏆 <b>TOP 10 PAPAN PERINGKAT:</b>",
        "job_weekly_payment": (
            "💵 <b>PEMBERITAHUAN PEMBAYARAN:</b>\n"
            "Permintaan penukaran USDT akan diproses minggu ini.\n"
            "Silakan periksa akun Tovest Anda."
        ),

        # --- /setlang ---
        "setlang_usage": (
            "📌 Cara pakai: /setlang vi | en | id\n"
            "• vi - Tiếng Việt\n"
            "• en - English\n"
            "• id - Bahasa Indonesia"
        ),
        "setlang_invalid": "⚠️ Bahasa tidak valid. Pilih: vi, en, id",
        "setlang_success": "✅ Bahasa diatur ke: <b>{lang_name}</b>",
        "lang_name_vi": "Tiếng Việt 🇻🇳",
        "lang_name_en": "English 🇬🇧",
        "lang_name_id": "Bahasa Indonesia 🇮🇩",

        # --- Post bài vào group ---
        "post_btn_open_account": "📋 Buka Akun",
        "post_btn_join_community": "👥 Gabung Komunitas",
        "post_btn_contact_admin": "📞 Hubungi Admin",
        "post_btn_deposit": "💰 Deposit Sekarang",
        "post_usage": (
            "📌 Cara pakai:\n"
            "/post\n"
            "Isi postingan\n"
            "---\n"
            "account: link buka akun\n"
            "community: link gabung komunitas\n"
            "admin: link hubungi admin\n"
            "deposit: link deposit\n\n"
            "💡 Jika tidak ada --- maka link default digunakan."
        ),
        "post_success": "✅ Diposting ke <b>{success}</b> grup!\n❌ Gagal: <b>{fail}</b>",
        "post_no_groups": "⚠️ Belum ada grup terdaftar.",
        "schedule_post_usage": (
            "📌 Cara pakai:\n"
            "/schedule_post HH:MM\n"
            "Isi postingan\n"
            "---\n"
            "account: link\n"
            "community: link\n"
            "admin: link\n"
            "deposit: link"
        ),
        "schedule_post_invalid_time": (
            "⚠️ Format waktu tidak valid. Gunakan HH:MM (waktu Vietnam).\n"
            "Contoh: /schedule_post 14:30 Isi postingan"
        ),
        "schedule_post_success": (
            "✅ Postingan dijadwalkan!\n"
            "🆔 ID: <b>{post_id}</b>\n"
            "⏰ Waktu: <b>{time}</b> (waktu Vietnam)\n"
            "📝 Isi: {content}"
        ),
        "scheduled_posts_title": "📋 <b>DAFTAR POSTINGAN TERJADWAL</b>\n",
        "scheduled_posts_empty": "📭 Tidak ada postingan terjadwal.",
        "scheduled_posts_item": "🆔 <b>{post_id}</b> | ⏰ {time} | 📝 {content}",
        "cancel_post_usage": "📌 Cara pakai: /cancel_post ID",
        "cancel_post_not_found": "⚠️ Postingan terjadwal tidak ditemukan dengan ID: {post_id}",
        "cancel_post_success": "✅ Postingan terjadwal ID: <b>{post_id}</b> dibatalkan.",
        "scheduled_post_sent": "📢 Postingan terjadwal ID {post_id} terkirim: ✅ {success} grup, ❌ {fail} gagal.",

        # --- Private reply ---
        "private_reply_sent": "✅ Info telah dikirim ke chat pribadi Anda.",
        "private_reply_error": "⚠️ Tidak bisa mengirim pesan pribadi. Silakan /start @{bot_username} terlebih dahulu.",
    },
}


def get_text(key: str, lang: str = DEFAULT_LANG, **kwargs) -> str:
    """
    Lấy text theo ngôn ngữ. Fallback về tiếng Việt nếu key không tồn tại.
    Hỗ trợ format string với **kwargs.
    """
    text = LANG.get(lang, LANG[DEFAULT_LANG]).get(key)
    if text is None:
        text = LANG[DEFAULT_LANG].get(key, key)
    if kwargs:
        try:
            text = text.format(**kwargs)
        except (KeyError, IndexError):
            pass
    return text


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
    """Khởi tạo tất cả bảng cần thiết (bao gồm bảng ngôn ngữ)."""
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

        -- Bảng lưu ngôn ngữ cho group/user (chat_id dương = user, âm = group)
        CREATE TABLE IF NOT EXISTS lang_settings (
            chat_id  INTEGER PRIMARY KEY,
            lang     TEXT DEFAULT 'vi'
        );

        -- Bảng lưu bài hẹn giờ (scheduled posts)
        CREATE TABLE IF NOT EXISTS scheduled_posts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            content     TEXT NOT NULL,
            scheduled_time TEXT NOT NULL,
            status      TEXT DEFAULT 'pending',
            links       TEXT DEFAULT NULL,
            created_at  TEXT DEFAULT (datetime('now'))
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
    return user.username and user.username.lower() in ADMIN_USERNAMES


def is_poster(user) -> bool:
    """Kiểm tra user có quyền post bài không (admin + post users)."""
    return is_admin(user)


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


# --- Hàm lấy/set ngôn ngữ ---

def get_lang(chat_id: int) -> str:
    """Lấy ngôn ngữ đã set cho chat (group hoặc user). Mặc định 'vi'."""
    conn = get_db()
    row = conn.execute(
        "SELECT lang FROM lang_settings WHERE chat_id = ?", (chat_id,)
    ).fetchone()
    conn.close()
    return row["lang"] if row else DEFAULT_LANG


def set_lang(chat_id: int, lang: str):
    """Lưu ngôn ngữ cho chat (group hoặc user)."""
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO lang_settings (chat_id, lang) VALUES (?, ?)",
        (chat_id, lang)
    )
    conn.commit()
    conn.close()


def chat_lang(update: Update) -> str:
    """Lấy ngôn ngữ phù hợp: group dùng lang của group, private dùng lang của user."""
    chat = update.effective_chat
    if chat.type in ("group", "supergroup"):
        return get_lang(chat.id)
    else:
        return get_lang(update.effective_user.id)


def get_group_lang(chat_id: int) -> str:
    """Lấy ngôn ngữ của group (dùng cho scheduled jobs)."""
    return get_lang(chat_id)


# ============================================================
# HELPER: Tạo inline keyboard cho bài post (theo ngôn ngữ)
# ============================================================

def parse_post_links(text: str) -> tuple:
    """
    Parse nội dung bài post và custom links.
    Format: Nội dung\n---\naccount: link\ncommunity: link\nadmin: link\ndeposit: link
    Trả về (content, links_dict). links_dict chứa các key: account, community, admin, deposit.
    Nếu không có --- thì dùng link mặc định.
    """
    links = {
        "account": POST_URL_OPEN_ACCOUNT,
        "community": POST_URL_JOIN_COMMUNITY,
        "admin": POST_URL_CONTACT_ADMIN,
        "deposit": POST_URL_DEPOSIT,
    }
    if "---" not in text:
        return text.strip(), links

    parts = text.split("---", 1)
    content = parts[0].strip()
    link_section = parts[1].strip()

    for line in link_section.split("\n"):
        line = line.strip()
        if ":" in line:
            key, value = line.split(":", 1)
            key = key.strip().lower()
            value = value.strip()
            if key in links and value:
                links[key] = value

    return content, links


def build_post_keyboard(lang: str, links: dict = None) -> InlineKeyboardMarkup:
    """Tạo 4 inline buttons cho bài post theo ngôn ngữ của group."""
    if links is None:
        links = {
            "account": POST_URL_OPEN_ACCOUNT,
            "community": POST_URL_JOIN_COMMUNITY,
            "admin": POST_URL_CONTACT_ADMIN,
            "deposit": POST_URL_DEPOSIT,
        }
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            get_text("post_btn_open_account", lang), url=links["account"]
        )],
        [InlineKeyboardButton(
            get_text("post_btn_join_community", lang), url=links["community"]
        )],
        [InlineKeyboardButton(
            get_text("post_btn_contact_admin", lang), url=links["admin"]
        )],
        [InlineKeyboardButton(
            get_text("post_btn_deposit", lang), url=links["deposit"]
        )],
    ])


# ============================================================
# COMMAND HANDLERS
# ============================================================

async def cmd_setlang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý /setlang - Chỉ admin @leyleyeyy mới dùng được."""
    lang = chat_lang(update)

    # Kiểm tra quyền admin
    if not is_admin(update.effective_user):
        await update.message.reply_text(get_text("admin_no_perm", lang), parse_mode=ParseMode.HTML)
        return

    # Kiểm tra tham số
    if not context.args:
        await update.message.reply_text(get_text("setlang_usage", lang), parse_mode=ParseMode.HTML)
        return

    new_lang = context.args[0].lower().strip()
    if new_lang not in ("vi", "en", "id"):
        await update.message.reply_text(get_text("setlang_invalid", lang), parse_mode=ParseMode.HTML)
        return

    # Xác định chat_id để lưu (group hoặc private)
    chat = update.effective_chat
    if chat.type in ("group", "supergroup"):
        target_id = chat.id
    else:
        target_id = update.effective_user.id

    set_lang(target_id, new_lang)

    # Lấy tên ngôn ngữ theo ngôn ngữ MỚI
    lang_name = get_text(f"lang_name_{new_lang}", new_lang)
    await update.message.reply_text(
        get_text("setlang_success", new_lang, lang_name=lang_name),
        parse_mode=ParseMode.HTML
    )


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

    lang = chat_lang(update)
    text = get_text("start_welcome", lang, name=display_name(user))
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_checkin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý /checkin - Hiển thị nút check-in."""
    # Lưu group
    chat = update.effective_chat
    if chat.type in ("group", "supergroup"):
        save_group(chat.id, chat.title or "")

    lang = chat_lang(update)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(get_text("btn_checkin", lang), callback_data="checkin")],
        [InlineKeyboardButton(get_text("btn_rules", lang), callback_data="rules")]
    ])
    await update.message.reply_text(
        get_text("checkin_prompt", lang),
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard
    )


async def callback_checkin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý callback khi user bấm nút Check-in."""
    query = update.callback_query
    await query.answer()
    user = query.from_user
    today = vn_today()
    lang = chat_lang(update)

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
        await query.answer(get_text("checkin_already", lang), show_alert=True)
        return

    # Tính streak
    yesterday = (datetime.now(VN_TZ) - timedelta(days=1)).strftime("%Y-%m-%d")
    if u["last_checkin"] == yesterday:
        new_streak = u["streak"] + 1
    elif u["last_checkin"] == today:
        conn.close()
        await query.answer(get_text("checkin_already", lang), show_alert=True)
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
    name = display_name(user)
    text = get_text("checkin_success", lang, name=name) + "\n\n"
    text += get_text("checkin_streak", lang, streak=new_streak) + "\n"
    text += get_text("checkin_points", lang, points=points)
    if milestone_bonus:
        text += "\n" + get_text("checkin_milestone", lang, streak=new_streak, bonus=milestone_bonus)
    text += "\n" + get_text("checkin_total_today", lang, total=total_points)

    # Lấy tổng điểm mới
    conn2 = get_db()
    row = conn2.execute("SELECT points FROM users WHERE user_id = ?", (user.id,)).fetchone()
    conn2.close()
    if row:
        text += "\n\n" + get_text("checkin_total_points", lang, points=row["points"])

    await query.message.reply_text(text, parse_mode=ParseMode.HTML)


async def callback_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý callback khi user bấm nút Quy tắc."""
    query = update.callback_query
    await query.answer()
    lang = chat_lang(update)
    rules = get_text("rules_text", lang, bot=BOT_USERNAME)
    await query.message.reply_text(rules, parse_mode=ParseMode.HTML)


async def cmd_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý /rules - Hiển thị quy tắc."""
    lang = chat_lang(update)
    rules = get_text("rules_text", lang, bot=BOT_USERNAME)
    await update.message.reply_text(rules, parse_mode=ParseMode.HTML)


async def cmd_myinfo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý /myinfo - Thông tin cá nhân."""
    user = update.effective_user
    u = get_or_create_user(user.id, user.username or "", user.full_name or "")
    lang = chat_lang(update)

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
    last_checkin = u["last_checkin"] or get_text("myinfo_no_checkin", lang)

    text = get_text("myinfo_title", lang, name=display_name(user)) + "\n"
    text += get_text("myinfo_points", lang, points=u["points"]) + "\n"
    text += get_text("myinfo_streak", lang, streak=u["streak"]) + "\n"
    text += get_text("myinfo_checkins", lang, count=total_checkins) + "\n"
    text += get_text("myinfo_referrals", lang, count=total_referrals) + "\n"
    text += get_text("myinfo_redeemed", lang, usdt=total_redeemed) + "\n"
    text += get_text("myinfo_can_redeem", lang,
                     count=redeem_available,
                     usdt=redeem_available * USDT_PER_REDEEM) + "\n\n"
    text += get_text("myinfo_last_checkin", lang, date=last_checkin)

    keyboard = None
    if redeem_available > 0:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                get_text("btn_redeem", lang, usdt=USDT_PER_REDEEM, points=POINTS_PER_REDEEM),
                callback_data="redeem"
            )]
        ])

    # Nếu trong group → gửi qua chat riêng
    if update.effective_chat.type != "private":
        try:
            await context.bot.send_message(
                chat_id=user.id, text=text,
                parse_mode=ParseMode.HTML, reply_markup=keyboard
            )
            await update.message.reply_text(
                get_text("private_reply_sent", lang),
                parse_mode=ParseMode.HTML
            )
        except Exception:
            await update.message.reply_text(
                get_text("private_reply_error", lang, bot_username=BOT_USERNAME),
                parse_mode=ParseMode.HTML
            )
    else:
        await update.message.reply_text(
            text, parse_mode=ParseMode.HTML, reply_markup=keyboard
        )


async def callback_redeem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý quy đổi USDT."""
    query = update.callback_query
    await query.answer()
    user = query.from_user
    lang = chat_lang(update)

    conn = get_db()
    u = conn.execute("SELECT * FROM users WHERE user_id = ?", (user.id,)).fetchone()

    if not u or u["points"] < POINTS_PER_REDEEM:
        conn.close()
        await query.answer(
            get_text("redeem_not_enough", lang, points=POINTS_PER_REDEEM),
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

    text = get_text("redeem_success", lang,
                    points=POINTS_PER_REDEEM,
                    usdt=USDT_PER_REDEEM,
                    remaining=new_points)
    await query.message.reply_text(text, parse_mode=ParseMode.HTML)
    logger.info(f"Redeem: {user.id} đổi {POINTS_PER_REDEEM}đ → {USDT_PER_REDEEM} USDT")


async def cmd_myreferral(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý /myreferral - Link mời bạn bè."""
    user = update.effective_user
    lang = chat_lang(update)
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{user.id}"
    text = get_text("referral_link", lang, link=ref_link)
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_referral_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý /referral_info - Thống kê mời bạn bè."""
    user = update.effective_user
    lang = chat_lang(update)

    conn = get_db()
    refs = conn.execute(
        "SELECT r.*, u.username, u.full_name FROM referrals r "
        "LEFT JOIN users u ON r.referred_id = u.user_id "
        "WHERE r.referrer_id = ? ORDER BY r.created_at DESC",
        (user.id,)
    ).fetchall()
    conn.close()

    if not refs:
        await update.message.reply_text(get_text("referral_empty", lang))
        return

    total = len(refs)
    qualified = sum(1 for r in refs if r["checkin_count"] >= 3)
    points_earned = qualified * 10

    text = get_text("referral_stats_title", lang) + "\n"
    text += get_text("referral_stats_total", lang, count=total) + "\n"
    text += get_text("referral_stats_qualified", lang, count=qualified) + "\n"
    text += get_text("referral_stats_points", lang, points=points_earned) + "\n\n"
    text += get_text("referral_stats_detail", lang) + "\n"

    for i, r in enumerate(refs[:20], 1):
        name = r["full_name"] or r["username"] or f"User#{r['referred_id']}"
        status = "✅" if r["checkin_count"] >= 3 else f"⏳ {r['checkin_count']}/3"
        text += f"{i}. {name} - {status}\n"

    if total > 20:
        text += "\n" + get_text("referral_stats_more", lang, count=total - 20)

    # Nếu trong group → gửi qua chat riêng
    if update.effective_chat.type != "private":
        try:
            await context.bot.send_message(
                chat_id=user.id, text=text,
                parse_mode=ParseMode.HTML
            )
            await update.message.reply_text(
                get_text("private_reply_sent", lang),
                parse_mode=ParseMode.HTML
            )
        except Exception:
            await update.message.reply_text(
                get_text("private_reply_error", lang, bot_username=BOT_USERNAME),
                parse_mode=ParseMode.HTML
            )
    else:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_event(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý /event - Gửi event link."""
    lang = chat_lang(update)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(get_text("btn_join_event", lang), callback_data="event_click")],
    ])
    await update.message.reply_text(
        get_text("event_title", lang),
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard
    )


async def callback_event_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Tracking click event link và gửi link cho user."""
    query = update.callback_query
    user = query.from_user
    lang = chat_lang(update)

    # Ghi nhận click
    conn = get_db()
    conn.execute(
        "INSERT INTO event_clicks (user_id, username) VALUES (?, ?)",
        (user.id, user.username or "")
    )
    conn.commit()
    conn.close()

    await query.answer(get_text("event_click_alert", lang), show_alert=False)
    await query.message.reply_text(
        get_text("event_click_msg", lang, name=display_name(user), link=EVENT_LINK),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )


async def cmd_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý /leaderboard - Bảng xếp hạng."""
    lang = chat_lang(update)
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

    text = get_text("lb_title", lang) + "\n"
    text += get_text("lb_top_points", lang) + "\n"
    for i, u in enumerate(top_points):
        medal = medals[i] if i < 3 else f"{i+1}."
        name = u["full_name"] or u["username"] or f"User#{u['user_id']}"
        text += get_text("lb_points_fmt", lang,
                         medal=medal, name=name,
                         points=u["points"], streak=u["streak"]) + "\n"

    if not top_points:
        text += get_text("lb_no_data", lang) + "\n"

    text += "\n" + get_text("lb_top_streak", lang) + "\n"
    for i, u in enumerate(top_streak):
        medal = medals[i] if i < 3 else f"{i+1}."
        name = u["full_name"] or u["username"] or f"User#{u['user_id']}"
        text += get_text("lb_streak_fmt", lang,
                         medal=medal, name=name,
                         streak=u["streak"], points=u["points"]) + "\n"

    if not top_streak:
        text += get_text("lb_no_data", lang) + "\n"

    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


# ============================================================
# ADMIN COMMANDS
# ============================================================

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: /stats - Thống kê click event."""
    lang = chat_lang(update)
    if not is_admin(update.effective_user):
        await update.message.reply_text(get_text("admin_no_perm", lang), parse_mode=ParseMode.HTML)
        return

    conn = get_db()
    total = conn.execute("SELECT COUNT(*) as cnt FROM event_clicks").fetchone()["cnt"]
    today_cnt = conn.execute(
        "SELECT COUNT(*) as cnt FROM event_clicks WHERE date(clicked_at) = date('now')"
    ).fetchone()["cnt"]
    unique = conn.execute(
        "SELECT COUNT(DISTINCT user_id) as cnt FROM event_clicks"
    ).fetchone()["cnt"]
    top = conn.execute(
        "SELECT user_id, username, COUNT(*) as cnt FROM event_clicks "
        "GROUP BY user_id ORDER BY cnt DESC LIMIT 10"
    ).fetchall()
    conn.close()

    text = get_text("stats_title", lang) + "\n"
    text += get_text("stats_total", lang, count=total) + "\n"
    text += get_text("stats_today", lang, count=today_cnt) + "\n"
    text += get_text("stats_unique", lang, count=unique) + "\n\n"
    text += get_text("stats_top_clickers", lang) + "\n"
    for i, r in enumerate(top, 1):
        name = r["username"] or f"User#{r['user_id']}"
        text += f"{i}. @{name} - {r['cnt']} clicks\n"

    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_checkin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: /checkin_stats - Thống kê check-in."""
    lang = chat_lang(update)
    if not is_admin(update.effective_user):
        await update.message.reply_text(get_text("admin_no_perm", lang), parse_mode=ParseMode.HTML)
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

    text = get_text("cstats_title", lang) + "\n"
    text += get_text("cstats_users", lang, count=total_users) + "\n"
    text += get_text("cstats_total", lang, count=total_checkins) + "\n"
    text += get_text("cstats_today", lang, count=today_checkins) + "\n"
    text += get_text("cstats_avg_streak", lang, avg=avg_streak) + "\n"
    text += get_text("cstats_max_streak", lang, max=max_streak) + "\n"
    text += get_text("cstats_total_points", lang, points=total_points)
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_referral_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: /referral_stats - Thống kê referral."""
    lang = chat_lang(update)
    if not is_admin(update.effective_user):
        await update.message.reply_text(get_text("admin_no_perm", lang), parse_mode=ParseMode.HTML)
        return

    conn = get_db()
    total = conn.execute("SELECT COUNT(*) as cnt FROM referrals").fetchone()["cnt"]
    qualified = conn.execute(
        "SELECT COUNT(*) as cnt FROM referrals WHERE checkin_count >= 3"
    ).fetchone()["cnt"]
    rewarded = conn.execute(
        "SELECT COUNT(*) as cnt FROM referrals WHERE rewarded = 1"
    ).fetchone()["cnt"]
    top = conn.execute(
        "SELECT r.referrer_id, u.username, u.full_name, COUNT(*) as cnt "
        "FROM referrals r LEFT JOIN users u ON r.referrer_id = u.user_id "
        "GROUP BY r.referrer_id ORDER BY cnt DESC LIMIT 10"
    ).fetchall()
    conn.close()

    text = get_text("rstats_title", lang) + "\n"
    text += get_text("rstats_total", lang, count=total) + "\n"
    text += get_text("rstats_qualified", lang, count=qualified) + "\n"
    text += get_text("rstats_rewarded", lang, count=rewarded) + "\n\n"
    text += get_text("rstats_top_referrers", lang) + "\n"
    for i, r in enumerate(top, 1):
        name = r["full_name"] or r["username"] or f"User#{r['referrer_id']}"
        text += f"{i}. {name} - {r['cnt']}\n"

    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_export_checkin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: /export_checkin - Xuất CSV check-in."""
    lang = chat_lang(update)
    if not is_admin(update.effective_user):
        await update.message.reply_text(get_text("admin_no_perm", lang), parse_mode=ParseMode.HTML)
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
        caption=get_text("export_checkin_caption", lang, date=vn_today())
    )


async def cmd_export_referral(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: /export_referral - Xuất CSV referral."""
    lang = chat_lang(update)
    if not is_admin(update.effective_user):
        await update.message.reply_text(get_text("admin_no_perm", lang), parse_mode=ParseMode.HTML)
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
        caption=get_text("export_referral_caption", lang, date=vn_today())
    )


async def cmd_payment_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: /payment_report - Báo cáo thanh toán USDT."""
    lang = chat_lang(update)
    if not is_admin(update.effective_user):
        await update.message.reply_text(get_text("admin_no_perm", lang), parse_mode=ParseMode.HTML)
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

    text = get_text("pay_title", lang) + "\n"
    text += get_text("pay_total_tx", lang, count=total_count) + "\n"
    text += get_text("pay_total_usdt", lang, usdt=total_usdt) + "\n\n"
    text += get_text("pay_detail", lang) + "\n"
    for i, r in enumerate(pending[:20], 1):
        name = r["full_name"] or r["username"] or f"User#{r['user_id']}"
        text += f"{i}. {name} - {r['total_usdt']:.2f} USDT ({r['cnt']}x)\n"

    if not pending:
        text += get_text("pay_no_tx", lang) + "\n"

    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: /users - Danh sách user."""
    lang = chat_lang(update)
    if not is_admin(update.effective_user):
        await update.message.reply_text(get_text("admin_no_perm", lang), parse_mode=ParseMode.HTML)
        return

    conn = get_db()
    users = conn.execute(
        "SELECT * FROM users ORDER BY points DESC LIMIT 50"
    ).fetchall()
    total = conn.execute("SELECT COUNT(*) as cnt FROM users").fetchone()["cnt"]
    conn.close()

    text = get_text("users_title", lang, total=total) + "\n"
    for i, u in enumerate(users, 1):
        name = u["full_name"] or u["username"] or f"User#{u['user_id']}"
        text += (
            f"{i}. {name} | {u['points']}pts | "
            f"🔥{u['streak']} | ID: {u['user_id']}\n"
        )

    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: /broadcast Nội dung - Gửi tin push cho tất cả user."""
    lang = chat_lang(update)
    if not is_admin(update.effective_user):
        await update.message.reply_text(get_text("admin_no_perm", lang), parse_mode=ParseMode.HTML)
        return

    if not context.args:
        await update.message.reply_text(get_text("broadcast_usage", lang))
        return

    content = " ".join(context.args)
    user_ids = get_all_users()
    success, fail = 0, 0

    for uid in user_ids:
        try:
            await context.bot.send_message(
                chat_id=uid,
                text=get_text("broadcast_header", lang, content=content),
                parse_mode=ParseMode.HTML
            )
            success += 1
        except Exception as e:
            fail += 1
            logger.warning(f"Broadcast fail to {uid}: {e}")

    await update.message.reply_text(
        get_text("broadcast_done", lang, success=success, fail=fail)
    )


# ============================================================
# ADMIN: POST BÀI VÀO GROUP
# ============================================================

# Lưu lỗi gần nhất để admin xem bằng /check_error
_last_post_errors = []


async def _send_post_to_groups(bot, content: str, links: dict = None) -> tuple:
    """
    Gửi bài post vào tất cả group đã đăng ký.
    Mỗi group hiển thị button theo ngôn ngữ của group đó.
    Trả về (success, fail).
    """
    global _last_post_errors
    _last_post_errors = []  # Reset lỗi mỗi lần post
    groups = get_all_groups()
    success, fail = 0, 0
    for chat_id in groups:
        try:
            lang = get_group_lang(chat_id)
            keyboard = build_post_keyboard(lang, links)
            await bot.send_message(
                chat_id=chat_id,
                text=content,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
                disable_web_page_preview=True
            )
            success += 1
        except Exception as e:
            fail += 1
            error_msg = str(e)
            _last_post_errors.append({
                "chat_id": chat_id,
                "error": error_msg,
                "time": vn_now().strftime("%Y-%m-%d %H:%M:%S"),
                "content_preview": content[:50],
                "links": links,
            })
            logger.error(f"Post thất bại cho group {chat_id}: {e}")
    return success, fail


async def cmd_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: /post Nội dung - Đăng bài ngay lập tức vào tất cả group."""
    lang = chat_lang(update)
    if not is_admin(update.effective_user):
        await update.message.reply_text(get_text("admin_no_perm", lang), parse_mode=ParseMode.HTML)
        return

    if not context.args:
        await update.message.reply_text(get_text("post_usage", lang), parse_mode=ParseMode.HTML)
        return

    # Lấy nội dung bài post và parse custom links
    raw_text = update.message.text.split(None, 1)[1]
    content, links = parse_post_links(raw_text)

    groups = get_all_groups()
    if not groups:
        await update.message.reply_text(get_text("post_no_groups", lang), parse_mode=ParseMode.HTML)
        return

    success, fail = await _send_post_to_groups(context.bot, content, links)
    result_text = get_text("post_success", lang, success=success, fail=fail)
    if fail > 0:
        result_text += "\n\n🔍 Dùng /check_error để xem chi tiết lỗi."
    await update.message.reply_text(result_text, parse_mode=ParseMode.HTML)
    logger.info(f"Admin post: {success} thành công, {fail} thất bại")


async def cmd_check_error(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: /check_error - Xem chi tiết lỗi post gần nhất."""
    lang = chat_lang(update)
    if not is_admin(update.effective_user):
        await update.message.reply_text(get_text("admin_no_perm", lang), parse_mode=ParseMode.HTML)
        return

    if not _last_post_errors:
        await update.message.reply_text(
            "✅ Không có lỗi nào. Lần post gần nhất thành công hoàn toàn!",
            parse_mode=ParseMode.HTML
        )
        return

    text = "🚨 <b>CHI TIẾT LỖI POST GẦN NHẤT</b>\n"
    text += f"📊 Tổng lỗi: <b>{len(_last_post_errors)}</b>\n\n"

    for i, err in enumerate(_last_post_errors[:10], 1):
        text += f"<b>Lỗi {i}:</b>\n"
        text += f"💬 Group ID: <code>{err['chat_id']}</code>\n"
        text += f"⏰ Thời gian: {err['time']}\n"
        text += f"❌ Lỗi: <code>{err['error'][:200]}</code>\n"
        if err.get('links'):
            text += f"🔗 Links: {err['links']}\n"
        text += "\n"

    if len(_last_post_errors) > 10:
        text += f"... và {len(_last_post_errors) - 10} lỗi khác.\n"

    text += "\n💡 <b>Nguyên nhân thường gặp:</b>\n"
    text += "• Link không hợp lệ (thiếu https://)\n"
    text += "• Bot chưa được cấp quyền admin trong group\n"
    text += "• Bot đã bị kick khỏi group\n"
    text += "• HTML format sai cú pháp"

    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_schedule_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý /schedule_post - Hẹn giờ đăng bài."""
    lang = chat_lang(update)
    if not is_poster(update.effective_user):
        await update.message.reply_text(get_text("admin_no_perm", lang), parse_mode=ParseMode.HTML)
        return

    if not context.args or len(context.args) < 2:
        await update.message.reply_text(get_text("schedule_post_usage", lang), parse_mode=ParseMode.HTML)
        return

    time_str = context.args[0]

    # Parse thời gian HH:MM
    try:
        hour, minute = map(int, time_str.split(":"))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError
    except (ValueError, AttributeError):
        await update.message.reply_text(
            get_text("schedule_post_invalid_time", lang), parse_mode=ParseMode.HTML
        )
        return

    # Lấy nội dung bài post và parse custom links
    raw_text = update.message.text.split(None, 2)
    if len(raw_text) < 3:
        await update.message.reply_text(get_text("schedule_post_usage", lang), parse_mode=ParseMode.HTML)
        return
    full_content = raw_text[2]
    content, links = parse_post_links(full_content)

    # Tính thời gian chạy: hôm nay hoặc ngày mai (nếu giờ đã qua)
    now = vn_now()
    scheduled_dt = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if scheduled_dt <= now:
        scheduled_dt += timedelta(days=1)  # Chuyển sang ngày mai

    scheduled_time_str = scheduled_dt.strftime("%Y-%m-%d %H:%M")

    # Lưu vào DB (lưu cả links dưới dạng JSON)
    import json as _json
    links_json = _json.dumps(links)
    conn = get_db()
    cur = conn.execute(
        "INSERT INTO scheduled_posts (content, scheduled_time, links) VALUES (?, ?, ?)",
        (content, scheduled_time_str, links_json)
    )
    post_id = cur.lastrowid
    conn.commit()
    conn.close()

    # Tính delay (giây) và đăng ký job
    delay = (scheduled_dt - now).total_seconds()
    context.application.job_queue.run_once(
        _job_execute_scheduled_post,
        when=delay,
        name=f"sched_post_{post_id}",
        data={"post_id": post_id, "content": content, "links": links}
    )

    # TrỪ nội dung dài cho hiển thị
    preview = content[:100] + ("..." if len(content) > 100 else "")
    await update.message.reply_text(
        get_text("schedule_post_success", lang,
                 post_id=post_id, time=scheduled_time_str, content=preview),
        parse_mode=ParseMode.HTML
    )
    logger.info(f"Scheduled post #{post_id} lúc {scheduled_time_str}")


async def cmd_scheduled_posts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý /scheduled_posts - Xem danh sách bài hẹn giờ."""
    lang = chat_lang(update)
    if not is_poster(update.effective_user):
        await update.message.reply_text(get_text("admin_no_perm", lang), parse_mode=ParseMode.HTML)
        return

    conn = get_db()
    rows = conn.execute(
        "SELECT id, content, scheduled_time FROM scheduled_posts "
        "WHERE status = 'pending' ORDER BY scheduled_time ASC"
    ).fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text(
            get_text("scheduled_posts_empty", lang), parse_mode=ParseMode.HTML
        )
        return

    text = get_text("scheduled_posts_title", lang) + "\n"
    for r in rows:
        preview = r["content"][:60] + ("..." if len(r["content"]) > 60 else "")
        text += get_text("scheduled_posts_item", lang,
                         post_id=r["id"], time=r["scheduled_time"],
                         content=preview) + "\n"

    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_cancel_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Xử lý /cancel_post - Hủy bài hẹn giờ."""
    lang = chat_lang(update)
    if not is_poster(update.effective_user):
        await update.message.reply_text(get_text("admin_no_perm", lang), parse_mode=ParseMode.HTML)
        return

    if not context.args:
        await update.message.reply_text(get_text("cancel_post_usage", lang), parse_mode=ParseMode.HTML)
        return

    try:
        post_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text(get_text("cancel_post_usage", lang), parse_mode=ParseMode.HTML)
        return

    conn = get_db()
    row = conn.execute(
        "SELECT id FROM scheduled_posts WHERE id = ? AND status = 'pending'",
        (post_id,)
    ).fetchone()

    if not row:
        conn.close()
        await update.message.reply_text(
            get_text("cancel_post_not_found", lang, post_id=post_id),
            parse_mode=ParseMode.HTML
        )
        return

    # Cập nhật trạng thái trong DB
    conn.execute(
        "UPDATE scheduled_posts SET status = 'cancelled' WHERE id = ?",
        (post_id,)
    )
    conn.commit()
    conn.close()

    # Hủy job trong JobQueue
    jobs = context.application.job_queue.get_jobs_by_name(f"sched_post_{post_id}")
    for job in jobs:
        job.schedule_removal()

    await update.message.reply_text(
        get_text("cancel_post_success", lang, post_id=post_id),
        parse_mode=ParseMode.HTML
    )
    logger.info(f"Cancelled scheduled post #{post_id}")


async def _job_execute_scheduled_post(context: ContextTypes.DEFAULT_TYPE):
    """
    Job callback: Gửi bài hẹn giờ vào tất cả group.
    Được gọi bởi JobQueue khi đến giờ.
    """
    data = context.job.data
    post_id = data["post_id"]
    content = data["content"]
    links = data.get("links", None)

    # Kiểm tra trạng thái (có thể đã bị hủy)
    conn = get_db()
    row = conn.execute(
        "SELECT status FROM scheduled_posts WHERE id = ?", (post_id,)
    ).fetchone()
    if not row or row["status"] != "pending":
        conn.close()
        logger.info(f"Scheduled post #{post_id} đã bị hủy hoặc không tồn tại, bỏ qua.")
        return

    # Cập nhật trạng thái thành 'sent'
    conn.execute(
        "UPDATE scheduled_posts SET status = 'sent' WHERE id = ?", (post_id,)
    )
    conn.commit()
    conn.close()

    # Gửi bài vào tất cả group
    success, fail = await _send_post_to_groups(context.bot, content, links)
    logger.info(f"Scheduled post #{post_id} đã gửi: {success} thành công, {fail} thất bại")


# ============================================================
# SCHEDULED JOBS (Tự động gửi vào group)
# ============================================================

async def job_send_event(context: ContextTypes.DEFAULT_TYPE):
    """Job: Gửi event link vào tất cả group."""
    groups = get_all_groups()
    if not groups:
        logger.info("Không có group nào để gửi event.")
        return

    now = vn_now().strftime("%H:%M")
    for chat_id in groups:
        try:
            lang = get_group_lang(chat_id)
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    get_text("btn_join_event", lang), callback_data="event_click"
                )]
            ])
            await context.bot.send_message(
                chat_id=chat_id,
                text=get_text("job_event", lang, time=now),
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

    conn = get_db()
    today_cnt = conn.execute(
        "SELECT COUNT(*) as cnt FROM checkins WHERE date = ?", (vn_today(),)
    ).fetchone()["cnt"]
    conn.close()

    for chat_id in groups:
        try:
            lang = get_group_lang(chat_id)
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    get_text("btn_checkin", lang), callback_data="checkin"
                )],
                [InlineKeyboardButton(
                    get_text("btn_rules", lang), callback_data="rules"
                )]
            ])
            await context.bot.send_message(
                chat_id=chat_id,
                text=get_text("job_reminder", lang, count=today_cnt),
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
    top = conn.execute(
        "SELECT u.user_id, u.username, u.full_name, u.points, u.streak "
        "FROM users u ORDER BY u.points DESC LIMIT 10"
    ).fetchall()
    total_users = conn.execute("SELECT COUNT(*) as cnt FROM users").fetchone()["cnt"]
    week_checkins = conn.execute(
        "SELECT COUNT(*) as cnt FROM checkins "
        "WHERE date >= date('now', '-7 days')"
    ).fetchone()["cnt"]
    pending_usdt = conn.execute(
        "SELECT COALESCE(SUM(usdt), 0) as total FROM redemptions"
    ).fetchone()["total"]
    conn.close()

    medals = ["🥇", "🥈", "🥉"]

    for chat_id in groups:
        try:
            lang = get_group_lang(chat_id)
            text = get_text("job_weekly_title", lang) + "\n"
            text += get_text("job_weekly_date", lang, date=vn_today()) + "\n\n"
            text += get_text("job_weekly_users", lang, count=total_users) + "\n"
            text += get_text("job_weekly_checkins", lang, count=week_checkins) + "\n"
            text += get_text("job_weekly_usdt", lang, usdt=pending_usdt) + "\n\n"
            text += get_text("job_weekly_top", lang) + "\n"

            for i, u in enumerate(top):
                medal = medals[i] if i < 3 else f"{i+1}."
                name = u["full_name"] or u["username"] or f"User#{u['user_id']}"
                text += get_text("lb_points_fmt", lang,
                                 medal=medal, name=name,
                                 points=u["points"], streak=u["streak"]) + "\n"

            text += "\n" + get_text("job_weekly_payment", lang)

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

def _restore_scheduled_posts(jq):
    """
    Khôi phục các bài hẹn giờ còn pending từ DB khi bot restart.
    Nếu thời gian đã qua thì đánh dấu 'expired', còn lại thì đăng ký lại job.
    """
    import json as _json
    conn = get_db()
    rows = conn.execute(
        "SELECT id, content, scheduled_time, links FROM scheduled_posts WHERE status = 'pending'"
    ).fetchall()
    conn.close()

    now = vn_now()
    for r in rows:
        try:
            scheduled_dt = datetime.strptime(r["scheduled_time"], "%Y-%m-%d %H:%M")
            scheduled_dt = scheduled_dt.replace(tzinfo=VN_TZ)
            delay = (scheduled_dt - now).total_seconds()

            # Parse links từ JSON
            links = None
            if r["links"]:
                try:
                    links = _json.loads(r["links"])
                except Exception:
                    pass

            if delay <= 0:
                # Thời gian đã qua → đánh dấu expired
                conn2 = get_db()
                conn2.execute(
                    "UPDATE scheduled_posts SET status = 'expired' WHERE id = ?",
                    (r["id"],)
                )
                conn2.commit()
                conn2.close()
                logger.info(f"Scheduled post #{r['id']} đã hết hạn, bỏ qua.")
            else:
                # Đăng ký lại job
                jq.run_once(
                    _job_execute_scheduled_post,
                    when=delay,
                    name=f"sched_post_{r['id']}",
                    data={"post_id": r["id"], "content": r["content"], "links": links}
                )
                logger.info(f"Restored scheduled post #{r['id']} lúc {r['scheduled_time']}")
        except Exception as e:
            logger.error(f"Lỗi khôi phục scheduled post #{r['id']}: {e}")


def setup_jobs(app: Application):
    """Thiết lập tất cả scheduled jobs + khôi phục bài hẹn giờ từ DB."""
    jq = app.job_queue

    # --- Khôi phục scheduled posts từ DB (sau khi restart) ---
    _restore_scheduled_posts(jq)

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
        BotCommand("start", "Start / Register"),
        BotCommand("checkin", "Daily check-in"),
        BotCommand("myinfo", "My info"),
        BotCommand("myreferral", "Referral link"),
        BotCommand("referral_info", "Referral stats"),
        BotCommand("leaderboard", "Leaderboard"),
        BotCommand("rules", "Program rules"),
        BotCommand("event", "Event link"),
        BotCommand("setlang", "Set language (admin)"),
        BotCommand("post", "Post to groups (admin)"),
        BotCommand("schedule_post", "Schedule post (admin)"),
        BotCommand("scheduled_posts", "View scheduled posts (admin)"),
        BotCommand("cancel_post", "Cancel scheduled post (admin)"),
        BotCommand("check_error", "Check post errors (admin)"),
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
    app.add_handler(CommandHandler("setlang", cmd_setlang))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("checkin_stats", cmd_checkin_stats))
    app.add_handler(CommandHandler("referral_stats", cmd_referral_stats))
    app.add_handler(CommandHandler("export_checkin", cmd_export_checkin))
    app.add_handler(CommandHandler("export_referral", cmd_export_referral))
    app.add_handler(CommandHandler("payment_report", cmd_payment_report))
    app.add_handler(CommandHandler("users", cmd_users))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    app.add_handler(CommandHandler("post", cmd_post))
    app.add_handler(CommandHandler("schedule_post", cmd_schedule_post))
    app.add_handler(CommandHandler("scheduled_posts", cmd_scheduled_posts))
    app.add_handler(CommandHandler("cancel_post", cmd_cancel_post))
    app.add_handler(CommandHandler("check_error", cmd_check_error))

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
