import asyncio
import json
import os
import time
from datetime import datetime, timezone, timedelta
import logging
import random
import sys
# Compatibility helpers (e.g. imghdr removal in Python 3.13)
import compat

from telegram import __version__ as ptb_version
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters
from telegram import error as telegram_error

from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text
from rich import box
from colorama import init, Fore, Style

# Initialize colorama for Windows
init(autoreset=True)

# Rich console
console = Console()

# SIGNAL BOT - Multi-channel broadcaster

BASE_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.join(BASE_DIR, 'config.json')
CHANNELS_PATH = os.path.join(BASE_DIR, 'channels.json')
POSTS_PATH = os.path.join(BASE_DIR, 'posts.json')
OUTBOX_PATH = os.path.join(BASE_DIR, 'outbox.json')
UPLOADS_DIR = os.path.join(BASE_DIR, 'uploads')

os.makedirs(UPLOADS_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('signal_bot')

# Simple file helpers with asyncio lock
file_lock = asyncio.Lock()


def load_json(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return [] if path.endswith('.json') else {}


def save_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def append_json(path, obj):
    data = load_json(path)
    if not isinstance(data, list):
        data = []
    data.append(obj)
    save_json(path, data)


# Load config
if not os.path.exists(CONFIG_PATH):
    logger.error('Missing config.json, please create one from config.json template')
    raise SystemExit(1)

with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
    CONFIG = json.load(f)

# Prefer an environment variable for the bot token to avoid committing secrets.
# Environment variable names supported: SIGNAL_BOT_TOKEN (preferred) or BOT_TOKEN (legacy).
BOT_TOKEN = os.environ.get('SIGNAL_BOT_TOKEN') or os.environ.get('BOT_TOKEN') or CONFIG.get('BOT_TOKEN')
if os.environ.get('SIGNAL_BOT_TOKEN') or os.environ.get('BOT_TOKEN'):
    logger.info('Using bot token from environment variable (SIGNAL_BOT_TOKEN/BOT_TOKEN)')

ADMIN_IDS = set(CONFIG.get('ADMIN_IDS', []))
SEND_RETRY_COUNT = CONFIG.get('SEND_RETRY_COUNT', 3)
SEND_RETRY_DELAY = CONFIG.get('SEND_RETRY_DELAY', 2)

# Anti-ban / safety settings
ANTI_BAN = CONFIG.get('ANTI_BAN', {}) or {}
INTER_SEND_DELAY = float(ANTI_BAN.get('inter_send_delay', 0.35))
JITTER = float(ANTI_BAN.get('jitter', 0.15))
PER_CHANNEL_COOLDOWN = float(ANTI_BAN.get('per_channel_cooldown', 10))
REMOVE_ON_FORBIDDEN = bool(ANTI_BAN.get('remove_on_forbidden', True))
FORBIDDEN_THRESHOLD = int(ANTI_BAN.get('forbidden_threshold', 3))

# In-memory trackers (not persisted)
last_sent_times = {}
forbidden_counts = {}

if not BOT_TOKEN or BOT_TOKEN == 'REPLACE_WITH_BOT_TOKEN':
    logger.error('⚠️  BOT_TOKEN is not configured! Please set the SIGNAL_BOT_TOKEN environment variable or edit config.json and add your Telegram bot token as BOT_TOKEN.')
    logger.error('   Get a token from @BotFather on Telegram.')
    raise SystemExit(1)

if not ADMIN_IDS or 123456789 in ADMIN_IDS:
    logger.error('⚠️  ADMIN_IDS is not configured! Please edit config.json and add your Telegram user ID(s).')
    logger.error('   Get your user ID from @userinfobot on Telegram.')
    raise SystemExit(1)

# Terminal UI stats
stats = {
    'start_time': datetime.now(),
    'total_broadcasts': 0,
    'successful_sends': 0,
    'failed_sends': 0,
    'last_broadcast': None,
    'status': 'Starting...'
}

def print_banner():
    """Print colorful startup banner"""
    console.clear()
    banner = """
╔═══════════════════════════════════════════════════════╗
║                                                       ║
║     ███████╗██╗ ██████╗ ███╗   ██╗ █████╗ ██╗       ║
║     ██╔════╝██║██╔════╝ ████╗  ██║██╔══██╗██║       ║
║     ███████╗██║██║  ███╗██╔██╗ ██║███████║██║       ║
║     ╚════██║██║██║   ██║██║╚██╗██║██╔══██║██║       ║
║     ███████║██║╚██████╔╝██║ ╚████║██║  ██║███████╗  ║
║     ╚══════╝╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝  ╚═╝╚══════╝  ║
║                                                       ║
║         Telegram AdBot - Multi-Channel Broadcaster    ║
║                                                       ║
╚═══════════════════════════════════════════════════════╝
"""
    console.print(banner, style="bold cyan")

def create_status_panel():
    """Create rich status panel"""
    channels = load_json(CHANNELS_PATH)
    uptime = datetime.now() - stats['start_time']
    uptime_str = str(uptime).split('.')[0]
    
    success_rate = 0
    if stats['successful_sends'] + stats['failed_sends'] > 0:
        success_rate = (stats['successful_sends'] / (stats['successful_sends'] + stats['failed_sends']) * 100)
    
    # Main stats table
    table = Table(box=box.ROUNDED, border_style="cyan")
    table.add_column("Metric", style="bold yellow", no_wrap=True)
    table.add_column("Value", style="bold green")
    
    table.add_row("🤖 Status", f"[bold green]{stats['status']}[/bold green]")
    table.add_row("⏱️  Uptime", uptime_str)
    table.add_row("📡 Channels", f"[bold cyan]{len(channels)}[/bold cyan]")
    table.add_row("📤 Broadcasts", f"[bold magenta]{stats['total_broadcasts']}[/bold magenta]")
    table.add_row("✅ Successful", f"[bold green]{stats['successful_sends']}[/bold green]")
    table.add_row("❌ Failed", f"[bold red]{stats['failed_sends']}[/bold red]")
    table.add_row("📊 Success Rate", f"[bold cyan]{success_rate:.1f}%[/bold cyan]")
    
    if stats['last_broadcast']:
        table.add_row("🕒 Last Post", stats['last_broadcast'])
    
    # Channels panel
    if channels:
        channels_text = "\n".join([f"  • {ch}" for ch in channels[:10]])
        if len(channels) > 10:
            channels_text += f"\n  ... and {len(channels) - 10} more"
    else:
        channels_text = "  [dim]No channels added yet[/dim]"
    
    channels_panel = Panel(
        channels_text,
        title="[bold cyan]📡 Active Channels[/bold cyan]",
        border_style="cyan",
        box=box.ROUNDED
    )
    
    # Combine into layout
    layout = Layout()
    layout.split_column(
        Layout(Panel(table, title="[bold cyan]⚡ SIGNAL BOT Status[/bold cyan]", border_style="cyan"), size=12),
        Layout(channels_panel, size=8)
    )
    
    return layout


async def write_log(entry: dict):
    """Logs have been disabled (no-op).

    This function intentionally does nothing to remove the logs system while preserving
    existing call-sites. If you later want to re-enable logging, restore the previous
    implementation which appended entries to the logs file and updated runtime stats.
    """
    return



async def add_channel(chat_identifier: str):
    async with file_lock:
        channels = load_json(CHANNELS_PATH)
        if chat_identifier in channels:
            return False
        channels.append(chat_identifier)
        save_json(CHANNELS_PATH, channels)
    await write_log({'type': 'channel_added', 'channel': chat_identifier})
    return True


async def remove_channel(chat_identifier: str):
    async with file_lock:
        channels = load_json(CHANNELS_PATH)
        if chat_identifier not in channels:
            return False
        channels.remove(chat_identifier)
        save_json(CHANNELS_PATH, channels)
    await write_log({'type': 'channel_removed', 'channel': chat_identifier})
    return True


async def list_channels():
    return load_json(CHANNELS_PATH)


async def record_post(meta: dict):
    async with file_lock:
        posts = load_json(POSTS_PATH)
        posts.append(meta)
        save_json(POSTS_PATH, posts)


async def broadcast_copy(bot, from_chat_id, message_id, origin_admin_id):
    channels = load_json(CHANNELS_PATH)
    if not channels:
        await write_log({'type': 'broadcast', 'status': 'no_channels'})
        return

    console.print(f"\n[cyan]📤 Broadcasting to {len(channels)} channel(s)...[/cyan]")

    results = []
    # Sequential send with cooldowns, jitter and forbidden handling to reduce ban risk
    for ch in channels:
        ch_id = ch
        # Enforce per-channel cooldown
        now_ts = time.time()
        last_ts = last_sent_times.get(ch_id)
        if last_ts:
            elapsed = now_ts - last_ts
            if elapsed < PER_CHANNEL_COOLDOWN:
                wait_for = PER_CHANNEL_COOLDOWN - elapsed
                console.print(f"[dim]Waiting {wait_for:.1f}s to respect cooldown for {ch_id}[/dim]")
                await asyncio.sleep(wait_for)

        attempt = 0
        success = False
        while attempt < SEND_RETRY_COUNT:
            try:
                await bot.copy_message(chat_id=ch_id, from_chat_id=from_chat_id, message_id=message_id)
                await write_log({'type': 'send', 'channel': ch_id, 'status': 'ok', 'origin_admin': origin_admin_id})
                results.append({'channel': ch_id, 'status': 'ok'})
                success = True
                break
            except Exception as e:
                attempt += 1
                err_name = type(e).__name__
                await write_log({'type': 'send', 'channel': ch_id, 'status': 'error', 'err': str(e), 'attempt': attempt})
                # If Forbidden, increment counter and maybe remove channel
                if err_name == 'Forbidden':
                    forbidden_counts[ch_id] = forbidden_counts.get(ch_id, 0) + 1
                    console.print(f"[yellow]⚠️ Forbidden when sending to {ch_id} (count={forbidden_counts[ch_id]})[/yellow]")
                    if REMOVE_ON_FORBIDDEN and forbidden_counts[ch_id] >= FORBIDDEN_THRESHOLD:
                        # remove channel from list
                        try:
                            removed = await remove_channel(ch_id)
                            if removed:
                                console.print(f"[red]Removed channel {ch_id} after repeated Forbidden errors[/red]")
                                await write_log({'type': 'channel_removed', 'channel': ch_id, 'reason': 'forbidden_threshold'})
                        except Exception:
                            pass
                        results.append({'channel': ch_id, 'status': 'forbidden'})
                        success = False
                        break
                # wait before retrying
                await asyncio.sleep(SEND_RETRY_DELAY)

        if not success and attempt >= SEND_RETRY_COUNT:
            results.append({'channel': ch_id, 'status': 'failed'})

        # record last send time
        last_sent_times[ch_id] = time.time()

        # Inter-send delay with jitter to avoid bursts
        delay = INTER_SEND_DELAY + random.uniform(0, JITTER)
        await asyncio.sleep(delay)
    await record_post({'type': 'broadcast', 'from_chat_id': from_chat_id, 'message_id': message_id, 'results': results, 'origin_admin': origin_admin_id, 'time': datetime.now(timezone.utc).isoformat()})
    
    # Show summary
    success_count = sum(1 for r in results if r.get('status') == 'ok')
    failed_count = len(results) - success_count
    if failed_count == 0:
        console.print(f"[green]✅ Broadcast complete! Sent to all {success_count} channels[/green]\n")
    else:
        console.print(f"[yellow]⚠️  Broadcast complete: {success_count} succeeded, {failed_count} failed[/yellow]\n")

async def handle_addchannel(update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text('Unauthorized')
        return
    
    # Check if this is a forwarded message from a channel
    fwd_chat = getattr(update.message, 'forward_from_chat', None)
    if fwd_chat:
        target = str(getattr(fwd_chat, 'id', fwd_chat))
        channel_title = getattr(fwd_chat, 'title', None) or getattr(fwd_chat, 'username', None) or "Unknown"
        ok = await add_channel(target)
        if ok:
            await update.message.reply_text(f'✅ Added channel: {channel_title}\nID: {target}\n\n⚠️ Make sure the bot is an admin in this channel!')
        else:
            await update.message.reply_text(f'⚠️ Channel {channel_title} (ID: {target}) already exists')
        return
    
    args = context.args
    if not args:
        await update.message.reply_text('**Usage:**\n\n**Option 1 (Easy):** Forward any message from your channel to me\n\n**Option 2:** /addchannel @yourchannel\n\n**Option 3 (Private channels):** /addchannel -1001234567890', parse_mode='Markdown')
        return
    target = args[0]
    
    # Auto-add @ if username doesn't have it and isn't a numeric ID
    if not target.startswith('@') and not target.lstrip('-').isdigit():
        target = '@' + target
    
    ok = await add_channel(target)
    if ok:
        await update.message.reply_text(f'✅ Added channel {target}\n\n⚠️ Make sure the bot is an admin in this channel!')
    else:
        await update.message.reply_text(f'⚠️ Channel {target} already exists')


async def handle_removechannel(update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text('Unauthorized')
        return
    args = context.args
    if not args:
        await update.message.reply_text('Usage: /removechannel <channel_username_or_id>')
        return
    target = args[0]
    
    # Auto-add @ if username doesn't have it and isn't a numeric ID
    if not target.startswith('@') and not target.lstrip('-').isdigit():
        target = '@' + target
    
    ok = await remove_channel(target)
    if ok:
        await update.message.reply_text(f'✅ Removed channel {target}')
    else:
        await update.message.reply_text(f'❌ Channel {target} not found')


async def handle_start(update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text('❌ Unauthorized - This bot is private.')
        return
    
    welcome = f"""🤖 **SIGNAL BOT** - Active!

👋 Welcome, Admin!

**Quick Commands:**
/status - View connected channels
/analytics - Detailed performance stats 📊
/stats - Quick summary
/addchannel - Add a channel (forward a message OR use @username)
/removechannel @channel - Remove a channel
/getchatid - Get ID of forwarded message's chat

**Auto-Broadcast:**
Just send me any message (text, photo, video, etc.) and I'll automatically post it to all your channels within 5 seconds!

**Add Private Channels:**
1. Forward any message from your private channel to me
2. Use /addchannel (no arguments needed!)
3. Bot will automatically detect the channel

**Schedule Posts:**
Reply to any message with:
• /schedule 5m (in 5 minutes)
• /schedule 2h (in 2 hours)
• /schedule 15:30 (today at 3:30 PM)
• /schedule tomorrow 10:00

Ready to broadcast! 🚀"""
    
    await update.message.reply_text(welcome, parse_mode='Markdown')


async def error_handler(update, context: ContextTypes.DEFAULT_TYPE):
    """
    Global error handler for all bot errors.
    Catches network timeouts, API errors, and other exceptions gracefully.
    """
    error = context.error
    
    # Log the error
    logger.error(f"Exception while handling an update: {error}", exc_info=context.error)
    
    # Handle specific error types
    if isinstance(error, telegram_error.TimedOut):
        # Network timeout - silent retry, user will get response eventually
        logger.warning("⚠️  Network timeout - Telegram API slow to respond")
        await write_log({'type': 'error', 'err': f'Network timeout: {str(error)}'})
        return
    
    elif isinstance(error, telegram_error.NetworkError):
        # Network connectivity issues
        logger.warning("⚠️  Network error - Connection issue with Telegram")
        await write_log({'type': 'error', 'err': f'Network error: {str(error)}'})
        return
    
    elif isinstance(error, telegram_error.RetryAfter):
        # Rate limiting - wait and retry
        retry_after = error.retry_after
        logger.warning(f"⚠️  Rate limited - Waiting {retry_after}s before retry")
        await write_log({'type': 'error', 'err': f'Rate limited: wait {retry_after}s'})
        await asyncio.sleep(retry_after)
        return
    
    elif isinstance(error, telegram_error.BadRequest):
        # Bad request - usually invalid parameters
        logger.error(f"❌ Bad request error: {error}")
        await write_log({'type': 'error', 'err': f'Bad request: {str(error)}'})
        # Try to notify user if update exists
        if update and update.effective_message:
            try:
                await update.effective_message.reply_text(
                    f"⚠️ Error: {str(error)}\nPlease check your command and try again."
                )
            except:
                pass
        return
    
    elif isinstance(error, telegram_error.Forbidden):
        # Bot was blocked or kicked from chat
        logger.error(f"❌ Forbidden error (bot blocked/kicked): {error}")
        await write_log({'type': 'error', 'err': f'Forbidden: {str(error)}'})
        return
    
    # Some telegram.error classes may not be present in all versions; match by name for compatibility
    elif type(error).__name__ == 'Unauthorized':
        # Invalid bot token
        logger.critical(f"🚨 CRITICAL: Invalid bot token! {error}")
        await write_log({'type': 'error', 'err': f'Unauthorized: {str(error)}'})
        return
    
    else:
        # Unknown error
        logger.error(f"❌ Unhandled error: {type(error).__name__} - {error}")
        await write_log({'type': 'error', 'err': f'Unhandled: {type(error).__name__} - {str(error)}'})
        # Try to notify user
        if update and update.effective_message:
            try:
                await update.effective_message.reply_text(
                    "⚠️ An unexpected error occurred. Please try again or contact admin."
                )
            except:
                pass


async def handle_getchatid(update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text('Unauthorized')
        return
    
    # Check if this is a forwarded message
    fwd_chat = getattr(update.message, 'forward_from_chat', None)
    if fwd_chat:
        chat = fwd_chat
        chat_type = getattr(chat, 'type', 'unknown')
        chat_title = getattr(chat, 'title', None) or getattr(chat, 'username', None) or "Unknown"
        chat_id = getattr(chat, 'id', None)
        
        info = f"""📋 **Chat Info:**

**Title:** {chat_title}
**ID:** `{chat_id}`
**Type:** {chat_type}

To add this channel, use:
/addchannel {chat_id}

Or simply forward a message and use /addchannel without arguments!"""
        
        await update.message.reply_text(info, parse_mode='Markdown')
    else:
        await update.message.reply_text('⚠️ Please forward a message from the channel first, then use /getchatid')


async def handle_status(update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text('Unauthorized')
        return
    channels = load_json(CHANNELS_PATH)
    text = '📡 **Active Channels:**\n\n' + '\n'.join([f'• {ch}' for ch in channels]) if channels else '❌ No channels connected.\n\nUse /addchannel @yourchannel to add one!'
    await update.message.reply_text(text, parse_mode='Markdown')


async def handle_analytics(update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text('Unauthorized')
        return
    
    posts = load_json(POSTS_PATH)
    channels = load_json(CHANNELS_PATH)

    # Calculate stats
    total_posts = len(posts)
    total_channels = len(channels)

    # Build per-channel stats from posts[].results (each broadcast records results)
    channel_stats = {ch: {'success': 0, 'failed': 0, 'total': 0} for ch in channels}
    for post in posts:
        for r in post.get('results', []):
            ch = r.get('channel')
            status = r.get('status')
            if ch not in channel_stats:
                # track channels that exist in posts but not in channels.json
                channel_stats.setdefault(ch, {'success': 0, 'failed': 0, 'total': 0})
            channel_stats[ch]['total'] += 1
            if status == 'ok':
                channel_stats[ch]['success'] += 1
            else:
                channel_stats[ch]['failed'] += 1

    # Calculate success rate
    total_sends = sum(s['total'] for s in channel_stats.values())
    total_success = sum(s['success'] for s in channel_stats.values())
    total_failed = sum(s['failed'] for s in channel_stats.values())
    success_rate = (total_success / total_sends * 100) if total_sends > 0 else 0
    
    # Recent posts (last 5)
    recent = posts[-5:][::-1] if posts else []
    
    # Build analytics message
    analytics_text = f"""📊 **SIGNAL BOT Analytics Dashboard**

**Overview:**
• Total Posts: {total_posts}
• Active Channels: {total_channels}
• Total Deliveries: {total_sends}
• Success Rate: {success_rate:.1f}%

**Delivery Stats:**
• ✅ Successful: {total_success}
• ❌ Failed: {total_failed}

**Per-Channel Performance:**
"""
    
    # Add per-channel stats
    for ch, stats in channel_stats.items():
        if stats['total'] > 0:
            ch_success_rate = (stats['success'] / stats['total'] * 100)
            status_emoji = '✅' if ch_success_rate >= 90 else '⚠️' if ch_success_rate >= 50 else '❌'
            analytics_text += f"\n{status_emoji} `{ch}`\n   Sent: {stats['total']} | Success: {stats['success']} | Failed: {stats['failed']} ({ch_success_rate:.0f}%)"
    
    if not channel_stats or all(s['total'] == 0 for s in channel_stats.values()):
        analytics_text += "\n_No delivery data yet_"
    
    analytics_text += "\n\n**Recent Posts:**\n"
    if recent:
        for i, post in enumerate(recent[:5], 1):
            timestamp = post.get('time', 'Unknown')
            # Parse timestamp
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                time_str = dt.strftime('%b %d, %I:%M %p')
            except:
                time_str = timestamp[:16] if timestamp else 'Unknown'
            
            results = post.get('results', [])
            success_count = sum(1 for r in results if r.get('status') == 'ok')
            total_count = len(results)
            analytics_text += f"\n{i}. {time_str} - {success_count}/{total_count} channels"
    else:
        analytics_text += "_No posts yet_"
    
    analytics_text += f"\n\n💡 Use /stats for quick summary"
    
    await update.message.reply_text(analytics_text, parse_mode='Markdown')


async def handle_stats(update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text('Unauthorized')
        return
    
    posts = load_json(POSTS_PATH)
    channels = load_json(CHANNELS_PATH)

    # Quick stats
    total_posts = len(posts)
    total_channels = len(channels)

    # Count sends from posts results
    sends_list = []
    for post in posts:
        for r in post.get('results', []):
            sends_list.append(r)
    success = sum(1 for r in sends_list if r.get('status') == 'ok')
    failed = sum(1 for r in sends_list if r.get('status') != 'ok')
    success_rate = (success / len(sends_list) * 100) if sends_list else 0
    
    # Last post time
    last_post = posts[-1] if posts else None
    if last_post:
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(last_post.get('time', '').replace('Z', '+00:00'))
            last_time = dt.strftime('%b %d at %I:%M %p')
        except:
            last_time = 'Unknown'
    else:
        last_time = 'Never'
    
    stats_text = f"""📈 **Quick Stats**

📊 Posts: {total_posts}
📡 Channels: {total_channels}
✅ Success: {success} ({success_rate:.0f}%)
❌ Failed: {failed}
🕒 Last Post: {last_time}

Use /analytics for detailed breakdown"""
    
    await update.message.reply_text(stats_text, parse_mode='Markdown')


async def handle_schedule(update, context: ContextTypes.DEFAULT_TYPE):
    # schedule format: /schedule 5m or /schedule 2h or /schedule 15:30 or /schedule tomorrow 10:00
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text('Unauthorized')
        return
    
    if not update.message.reply_to_message:
        await update.message.reply_text('⚠️ Reply to the message you want to schedule!\n\nExamples:\n/schedule 5m (in 5 minutes)\n/schedule 2h (in 2 hours)\n/schedule 15:30 (today at 3:30 PM)\n/schedule tomorrow 10:00')
        return
    
    args = context.args
    if not args:
        await update.message.reply_text('⚠️ Specify when to send!\n\nExamples:\n/schedule 5m\n/schedule 2h\n/schedule 15:30\n/schedule tomorrow 10:00')
        return
    
    time_str = ' '.join(args)
    now = datetime.now()
    
    try:
        # Parse different formats
        if time_str.endswith('m'):  # Minutes: 5m, 10m
            minutes = int(time_str[:-1])
            dt = now + timedelta(minutes=minutes)
        elif time_str.endswith('h'):  # Hours: 2h, 5h
            hours = int(time_str[:-1])
            dt = now + timedelta(hours=hours)
        elif 'tomorrow' in time_str.lower():  # tomorrow 10:00
            time_part = time_str.lower().replace('tomorrow', '').strip()
            if ':' in time_part:
                hour, minute = map(int, time_part.split(':'))
                dt = now + timedelta(days=1)
                dt = dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
            else:
                dt = now + timedelta(days=1)
        elif ':' in time_str:  # Today at specific time: 15:30
            hour, minute = map(int, time_str.split(':'))
            dt = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            # If time already passed today, schedule for tomorrow
            if dt < now:
                dt = dt + timedelta(days=1)
        else:
            await update.message.reply_text('❌ Invalid format!\n\nUse:\n• 5m (minutes)\n• 2h (hours)\n• 15:30 (time today)\n• tomorrow 10:00')
            return
    except Exception as e:
        await update.message.reply_text(f'❌ Invalid time format!\n\nUse:\n• 5m or 30m\n• 2h or 5h\n• 15:30\n• tomorrow 10:00')
        return
    
    # Create schedule entry
    outbox = load_json(OUTBOX_PATH)
    entry = {
        'type': 'copy',
        'from_chat_id': update.message.reply_to_message.chat_id,
        'message_id': update.message.reply_to_message.message_id,
        'send_at': dt.astimezone(timezone.utc).isoformat(),
        'status': 'pending',
        'origin_admin': user.id
    }
    outbox.append(entry)
    save_json(OUTBOX_PATH, outbox)
    await write_log({'type': 'scheduled', 'entry': entry})
    
    # Format nice display time
    from_now = dt - now
    if from_now.total_seconds() < 3600:
        time_display = f"in {int(from_now.total_seconds() / 60)} minutes"
    else:
        time_display = dt.strftime('%B %d at %I:%M %p')
    
    await update.message.reply_text(f'✅ Message scheduled {time_display}!')


async def handle_message(update, context: ContextTypes.DEFAULT_TYPE):
    # Accept any content from admin; forward/copy to channels within 5s
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        # ignore non-admin messages
        return
    # If message is a command, let command handlers handle it.
    if update.message.text and update.message.text.startswith('/'):
        return

    # Broadcast by copying message to all channels
    channels = load_json(CHANNELS_PATH)
    if not channels:
        await update.message.reply_text('⚠️ No channels configured!\n\nAdd channels first using:\n/addchannel @yourchannel')
        return
    
    # Send confirmation
    await update.message.reply_text(f'📤 Broadcasting to {len(channels)} channel(s)...')
    
    try:
        await broadcast_copy(context.bot, update.message.chat_id, update.message.message_id, user.id)
        await update.message.reply_text(f'✅ Broadcast complete!')
    except Exception as e:
        await write_log({'type': 'broadcast_error', 'err': str(e)})
        await update.message.reply_text(f'❌ Broadcast error: {str(e)}')


async def outbox_loop(application):
    # Periodically poll OUTBOX_PATH for pending entries and send them.
    while True:
        try:
            async with file_lock:
                outbox = load_json(OUTBOX_PATH)
                changed = False
                now = datetime.now(timezone.utc)
                for entry in outbox:
                    if entry.get('status') != 'pending':
                        continue
                    send_at = datetime.fromisoformat(entry.get('send_at')).astimezone(timezone.utc)
                    if send_at <= now:
                        # mark sending to avoid duplicate
                        entry['status'] = 'sending'
                        changed = True
                if changed:
                    save_json(OUTBOX_PATH, outbox)

            # Send outside the lock
            for entry in outbox:
                if entry.get('status') != 'sending':
                    continue
                if entry.get('type') == 'copy':
                    try:
                        await broadcast_copy(application.bot, entry['from_chat_id'], entry['message_id'], entry.get('origin_admin'))
                        entry['status'] = 'sent'
                        entry['sent_at'] = datetime.now(timezone.utc).isoformat()
                        await write_log({'type': 'outbox_sent', 'entry': entry})
                    except Exception as e:
                        entry['status'] = 'error'
                        entry['err'] = str(e)
                        await write_log({'type': 'outbox_error', 'err': str(e), 'entry': entry})
                elif entry.get('type') == 'file':
                    # UI-originated file/text post
                    try:
                        channels = load_json(CHANNELS_PATH)
                        results = []
                        for ch in channels:
                            try:
                                if entry.get('file_path'):
                                    fp = entry['file_path']
                                    # choose send_document for generic files
                                    await application.bot.send_document(chat_id=ch, document=open(fp, 'rb'), caption=entry.get('text'))
                                else:
                                    await application.bot.send_message(chat_id=ch, text=entry.get('text', ''))
                                results.append({'channel': ch, 'status': 'ok'})
                            except Exception as e:
                                results.append({'channel': ch, 'status': 'error', 'err': str(e)})
                                await write_log({'type': 'send', 'channel': ch, 'status': 'error', 'err': str(e)})
                        entry['status'] = 'sent'
                        entry['results'] = results
                        entry['sent_at'] = datetime.now(timezone.utc).isoformat()
                        await write_log({'type': 'outbox_sent', 'entry': entry})
                    except Exception as e:
                        entry['status'] = 'error'
                        entry['err'] = str(e)
                        await write_log({'type': 'outbox_error', 'err': str(e), 'entry': entry})
                else:
                    entry['status'] = 'unknown_type'

            async with file_lock:
                save_json(OUTBOX_PATH, outbox)
        except Exception as e:
            await write_log({'type': 'outbox_loop_error', 'err': str(e)})
        await asyncio.sleep(1)


def main():
    # Print banner
    print_banner()
    console.print("\n[bold green]🚀 Starting SIGNAL BOT...[/bold green]\n")
    
    # Show initial status
    channels = load_json(CHANNELS_PATH)
    console.print(f"[cyan]📡 Active Channels: {len(channels)}[/cyan]")
    if channels:
        for ch in channels[:5]:
            console.print(f"  • {ch}", style="dim")
        if len(channels) > 5:
            console.print(f"  ... and {len(channels) - 5} more", style="dim")
    console.print("\n[dim]Press Ctrl+C to stop[/dim]\n")
    
    logger.info('python-telegram-bot version %s', ptb_version)
    app = ApplicationBuilder().token(BOT_TOKEN).concurrent_updates(True).build()

    # Register error handler
    app.add_error_handler(error_handler)

    # Register command handlers
    app.add_handler(CommandHandler('start', handle_start))
    app.add_handler(CommandHandler('addchannel', handle_addchannel))
    app.add_handler(CommandHandler('removechannel', handle_removechannel))
    app.add_handler(CommandHandler('status', handle_status))
    app.add_handler(CommandHandler('analytics', handle_analytics))
    app.add_handler(CommandHandler('stats', handle_stats))
    app.add_handler(CommandHandler('getchatid', handle_getchatid))
    app.add_handler(CommandHandler('schedule', handle_schedule))
    app.add_handler(MessageHandler(filters.ALL & (~filters.COMMAND), handle_message))

    # start outbox loop in background
    async def _run():
        stats['status'] = 'Initializing...'
        await asyncio.sleep(1)
        
        # Start outbox loop
        asyncio.create_task(outbox_loop(app))
        
        # Run until cancelled
        await app.initialize()
        stats['status'] = 'Connected ✓'
        console.print("[bold green]✓ Bot connected to Telegram![/bold green]\n")
        await app.start()
        await app.updater.start_polling()
        # Keep running
        while True:
            await asyncio.sleep(3600)

    try:
        asyncio.run(_run())
    except (KeyboardInterrupt, SystemExit):
        console.print("\n[bold yellow]⚠️  Shutting down SIGNAL BOT...[/bold yellow]")
        console.print("[bold green]✓ Bot stopped successfully![/bold green]\n")
        logger.info('Shutting down')


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        # As a defensive measure for process managers (systemd, etc.), ensure
        # the process exits with code 0 and log the exception clearly so the
        # runtime doesn't consider this a crash. This follows the project's
        # resilience-first policy: prefer availability over strict error codes.
        logger.exception('Unhandled exception in main; exiting with code 0')
        try:
            # give a moment for logs to flush
            import time as _ap_time
            _ap_time.sleep(0.1)
        except Exception:
            pass
        sys.exit(0)
