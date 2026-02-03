"""
Get Telegram Chat ID for bot notifications.

Usage:
1. Start the bot: Search @brighter87_systrading_bot in Telegram and press Start
2. OR add the bot to your channel as admin
3. Send a message to the bot or channel
4. Run this script: python scripts/get_telegram_chat_id.py
5. Copy the chat_id to .env file
"""

import os
import requests
from pathlib import Path
from dotenv import load_dotenv

# Load .env
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(env_path)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

def get_updates():
    """Get recent messages/updates from the bot."""
    if not TELEGRAM_BOT_TOKEN:
        print("[ERROR] TELEGRAM_BOT_TOKEN not found in .env")
        return None

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"

    try:
        response = requests.get(url, timeout=10)
        data = response.json()

        if not data.get("ok"):
            print(f"[ERROR] API error: {data}")
            return None

        return data.get("result", [])

    except Exception as e:
        print(f"[ERROR] Failed to get updates: {e}")
        return None


def main():
    print("=" * 50)
    print("Telegram Chat ID Finder")
    print("=" * 50)

    print(f"\nBot Token: {TELEGRAM_BOT_TOKEN[:20]}..." if TELEGRAM_BOT_TOKEN else "Bot Token: NOT SET")

    updates = get_updates()

    if updates is None:
        return

    if not updates:
        print("\n[INFO] No messages found.")
        print("\nTo get your chat_id:")
        print("1. Open Telegram and search for your bot")
        print("2. Press 'Start' or send any message to the bot")
        print("3. Run this script again")
        print("\nFor a channel:")
        print("1. Add the bot to your channel as admin")
        print("2. Send a message to the channel")
        print("3. Run this script again")
        return

    print(f"\n[INFO] Found {len(updates)} message(s):\n")

    seen_chats = {}

    for update in updates:
        # Handle regular messages
        message = update.get("message") or update.get("channel_post")

        if message:
            chat = message.get("chat", {})
            chat_id = chat.get("id")
            chat_type = chat.get("type")

            # Get name based on type
            if chat_type == "private":
                name = f"{chat.get('first_name', '')} {chat.get('last_name', '')}".strip()
                name = name or chat.get("username", "Unknown")
            elif chat_type == "channel":
                name = chat.get("title", chat.get("username", "Unknown Channel"))
            elif chat_type in ("group", "supergroup"):
                name = chat.get("title", "Unknown Group")
            else:
                name = "Unknown"

            if chat_id not in seen_chats:
                seen_chats[chat_id] = {
                    "id": chat_id,
                    "type": chat_type,
                    "name": name,
                }

    if seen_chats:
        print("-" * 50)
        print(f"{'Chat ID':<20} {'Type':<12} {'Name'}")
        print("-" * 50)

        for chat_id, info in seen_chats.items():
            print(f"{info['id']:<20} {info['type']:<12} {info['name']}")

        print("-" * 50)
        print("\nCopy the chat_id you want to use and add it to .env:")
        print("TELEGRAM_CHAT_ID=<your_chat_id>")
    else:
        print("[INFO] No chat information found in updates.")


if __name__ == "__main__":
    main()
