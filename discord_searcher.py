import requests
import json
from datetime import datetime, timedelta
import time
import os
from collections import defaultdict
import random
import hashlib
from supabase import create_client, Client

# ========== CONFIGURATION ==========
# Use environment variables for secrets (set these in Render dashboard)
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
TOKEN = os.environ.get('DISCORD_TOKEN')

# Channel keywords to scan
CHANNEL_KEYWORDS = ['selfie', 'photo', 'intro', 'pictures', 'user-content', 'showoff', 'rate', 'selfies', 'fit', 'dom', 'sub', 'tops', 'bottoms', 'face', 'room-review', 'cosplay', 'nudes', 'twink', 'fem-', 'cum', 'dicks', 'smash', 'confessions', 'body', 'self', 'nude', 'confess']

# SCAN SETTINGS
INCLUDE_NSFW = True
MAX_MESSAGES_PER_CHANNEL = 5000  # Increased for better coverage
INCLUDE_ARCHIVED_THREADS = True
SCAN_ALL_SERVERS = True
TARGET_SERVER = "sissies 1"

# OPTIMIZATION SETTINGS
BATCH_SIZE = 100
PROFILE_BATCH_SIZE = 50
MIN_DELAY = 0.3
MAX_DELAY = 0.8

# ========== SUPABASE SETUP ==========
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Cache for existing users and messages
user_cache = set()
message_cache = set()

# ========== HELPER FUNCTIONS ==========

def log(msg):
    print(f"📝 {msg}")

def log_error(msg):
    print(f"❌ ERROR: {msg}")

def random_delay():
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

def contains_keyword(text):
    if not text:
        return False
    text = text.lower()
    return any(keyword in text for keyword in CHANNEL_KEYWORDS)

def get_headers():
    return {
        'authorization': TOKEN,
        'accept': '*/*',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

def make_request(url):
    headers = get_headers()
    try:
        random_delay()
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 5))
            log(f"⏳ Rate limited, waiting {retry_after}s")
            time.sleep(retry_after)
            return make_request(url)
        return None
    except Exception as e:
        log_error(f"Request failed: {e}")
        return None

def make_post_request(url, data=None):
    headers = get_headers()
    headers['content-type'] = 'application/json'
    try:
        random_delay()
        response = requests.post(url, headers=headers, json=data, timeout=10)
        return response.status_code == 200
    except:
        return False

def confirm_nsfw(channel_id):
    confirm_url = f"https://discord.com/api/v9/channels/{channel_id}/nsfw-confirm"
    success = make_post_request(confirm_url, {})
    if success:
        log(f"  ✅ NSFW confirmed for channel {channel_id}")
    return success

def get_all_servers():
    servers = make_request('https://discord.com/api/v9/users/@me/guilds')
    if servers:
        log(f"📋 Found {len(servers)} total servers")
        return servers
    return []

def get_server_channels(server_id):
    return make_request(f'https://discord.com/api/v9/guilds/{server_id}/channels') or []

def get_forum_threads(forum_id):
    """Get ALL threads from a forum channel"""
    all_threads = []
    
    # Get active threads
    url = f'https://discord.com/api/v9/channels/{forum_id}/threads/active'
    data = make_request(url)
    if data and 'threads' in data:
        all_threads.extend(data['threads'])
        log(f"      Found {len(data['threads'])} active threads")
    
    # Get archived threads (public)
    if INCLUDE_ARCHIVED_THREADS:
        archived_url = f'https://discord.com/api/v9/channels/{forum_id}/threads/archived/public'
        archived_data = make_request(archived_url)
        if archived_data and 'threads' in archived_data:
            all_threads.extend(archived_data['threads'])
            log(f"      Found {len(archived_data['threads'])} archived public threads")
        
        # Also check private archived threads
        private_archived_url = f'https://discord.com/api/v9/channels/{forum_id}/threads/archived/private'
        private_data = make_request(private_archived_url)
        if private_data and 'threads' in private_data:
            all_threads.extend(private_data['threads'])
            log(f"      Found {len(private_data['threads'])} archived private threads")
    
    return all_threads

def get_channel_messages(channel_id, limit=MAX_MESSAGES_PER_CHANNEL):
    """Get messages from ANY channel (including thread channels)"""
    messages = []
    last_id = None
    fetched = 0
    
    while True:
        url = f'https://discord.com/api/v9/channels/{channel_id}/messages?limit=100'
        if last_id:
            url += f'&before={last_id}'
        
        batch = make_request(url)
        if not batch:
            break
            
        messages.extend(batch)
        fetched += len(batch)
        
        if limit > 0 and fetched >= limit:
            messages = messages[:limit]
            break
            
        if len(batch) < 100:
            break
            
        last_id = batch[-1]['id']
        time.sleep(0.5)
    
    return messages

def get_account_creation_date(user_id):
    try:
        timestamp = ((int(user_id) >> 22) + 1420070400000) / 1000
        return datetime.fromtimestamp(timestamp).isoformat()
    except:
        return None

def extract_user_from_message(msg):
    author = msg.get('author', {})
    avatar_url = None
    if author.get('avatar'):
        avatar_hash = author['avatar']
        ext = 'gif' if avatar_hash.startswith('a_') else 'png'
        avatar_url = f"https://cdn.discordapp.com/avatars/{author['id']}/{avatar_hash}.{ext}?size=4096"
    banner_url = None
    if author.get('banner'):
        banner_hash = author['banner']
        ext = 'gif' if banner_hash.startswith('a_') else 'png'
        banner_url = f"https://cdn.discordapp.com/banners/{author['id']}/{banner_hash}.{ext}?size=4096"
    return {
        'user_id': author['id'],
        'username': author.get('username', 'unknown'),
        'global_name': author.get('global_name'),
        'avatar_url': avatar_url,
        'banner_url': banner_url,
        'accent_color': author.get('accent_color'),
        'public_flags': author.get('public_flags', 0),
        'account_created_at': get_account_creation_date(author['id'])
    }

def extract_media(msg):
    """Extract ALL media from a message - COMPREHENSIVE VERSION"""
    media = []
    
    # 1. ATTACHMENTS - Direct file uploads
    if msg.get('attachments'):
        for att in msg['attachments']:
            content_type = att.get('content_type', '')
            filename = att['filename']
            url = att['url']
            is_spoiler = att.get('spoiler', False) or filename.startswith('SPOILER_')
            
            if content_type.startswith('image/gif') or filename.lower().endswith('.gif'):
                media_type = 'spoiler_gif' if is_spoiler else 'gif'
                media.append({'type': media_type, 'url': url})
            elif content_type.startswith('image/'):
                media_type = 'spoiler_image' if is_spoiler else 'image'
                media.append({'type': media_type, 'url': url})
            elif content_type.startswith('video/'):
                media_type = 'spoiler_video' if is_spoiler else 'video'
                media.append({'type': media_type, 'url': url})
            else:
                media_type = 'spoiler_file' if is_spoiler else 'file'
                media.append({'type': media_type, 'url': url})
    
    # 2. EMBEDS - Rich content from links
    if msg.get('embeds'):
        for embed in msg['embeds']:
            # Image in embed
            if embed.get('image'):
                url = embed['image'].get('url') or embed['image'].get('proxy_url')
                if url:
                    media.append({'type': 'embed_image', 'url': url})
            
            # Video in embed
            if embed.get('video'):
                url = embed['video'].get('url') or embed['video'].get('proxy_url')
                if url:
                    media.append({'type': 'embed_video', 'url': url})
            
            # Thumbnail
            if embed.get('thumbnail'):
                url = embed['thumbnail'].get('url') or embed['thumbnail'].get('proxy_url')
                if url:
                    media.append({'type': 'thumbnail', 'url': url})
            
            # Direct image/video embeds
            if embed.get('type') in ['image', 'video'] and embed.get('url'):
                url = embed['url']
                if embed.get('type') == 'image':
                    media.append({'type': 'image_embed', 'url': url})
                else:
                    media.append({'type': 'video_embed', 'url': url})
    
    # 3. STICKERS
    if msg.get('sticker_items'):
        for sticker in msg['sticker_items']:
            sticker_url = f"https://cdn.discordapp.com/stickers/{sticker['id']}.png"
            media.append({'type': 'sticker', 'url': sticker_url})
    
    return media

def process_message(msg, server_name, server_id, channel_name, channel_id, thread_name=None):
    """Process a single message"""
    timestamp = datetime.fromisoformat(msg['timestamp'].replace('Z', '+00:00'))
    edited_timestamp = msg.get('edited_timestamp')
    if edited_timestamp:
        edited_timestamp = datetime.fromisoformat(edited_timestamp.replace('Z', '+00:00'))
    
    media = extract_media(msg)
    
    # Create media_urls and media_types arrays from media objects
    media_urls = [m['url'] for m in media]
    media_types = [m['type'] for m in media]
    
    return {
        'message_id': msg['id'],
        'server_id': server_id,
        'server_name': server_name,
        'channel_id': channel_id,
        'channel_name': channel_name,
        'thread_name': thread_name,
        'author_name': msg['author']['username'],
        'author_id': msg['author']['id'],
        'content': msg['content'] if msg['content'] else "",
        'media_urls': media_urls,
        'media_types': media_types,
        'timestamp': timestamp.isoformat(),
        'edited_timestamp': edited_timestamp.isoformat() if edited_timestamp else None,
        'is_edited': edited_timestamp is not None,
        'edit_count': 0,
        'first_seen': datetime.now().isoformat(),
        'last_seen': datetime.now().isoformat()
    }

# ========== DATABASE FUNCTIONS ==========

def batch_save_users(users_dict):
    """Save multiple users - skip duplicates, update existing"""
    if not users_dict:
        return True
    
    try:
        new_users = []
        existing_users = []
        
        for user_id, user_data in users_dict.items():
            if user_id in user_cache:
                existing_users.append(user_data)
            else:
                # Double-check with database
                try:
                    existing = supabase.table('users').select('user_id').eq('user_id', user_id).execute()
                    if existing.data:
                        existing_users.append(user_data)
                        user_cache.add(user_id)
                    else:
                        new_users.append(user_data)
                        user_cache.add(user_id)
                except:
                    new_users.append(user_data)
                    user_cache.add(user_id)
        
        # Update existing users
        if existing_users:
            for user in existing_users:
                try:
                    supabase.table('users').update({
                        'last_seen': datetime.now().isoformat(),
                        'username': user.get('username'),
                        'global_name': user.get('global_name'),
                        'avatar_url': user.get('avatar_url'),
                        'banner_url': user.get('banner_url'),
                        'public_flags': user.get('public_flags')
                    }).eq('user_id', user['user_id']).execute()
                except Exception as e:
                    pass
            print(f"  👤 Updated {len(existing_users)} existing users")
        
        # Insert new users
        if new_users:
            for user in new_users:
                user['first_seen'] = datetime.now().isoformat()
                user['last_seen'] = datetime.now().isoformat()
                user['last_updated'] = datetime.now().isoformat()
            
            for i in range(0, len(new_users), PROFILE_BATCH_SIZE):
                chunk = new_users[i:i+PROFILE_BATCH_SIZE]
                supabase.table('users').insert(chunk).execute()
            print(f"  👥 Inserted {len(new_users)} new users")
        
        return True
    except Exception as e:
        print(f"  ⚠️ User batch save failed: {e}")
        return False

def batch_save_messages(messages_batch):
    """Save multiple messages - CORRECTLY handles upserts"""
    if not messages_batch:
        return
    
    try:
        for msg in messages_batch:
            try:
                # Check if message exists
                existing = supabase.table('messages').select('message_id').eq('message_id', msg['message_id']).execute()
                
                if existing.data:
                    # UPDATE existing message
                    supabase.table('messages').update({
                        'last_seen': datetime.now().isoformat(),
                        'media_urls': msg['media_urls'],
                        'media_types': msg['media_types'],
                        'is_edited': msg['is_edited'],
                        'edited_timestamp': msg.get('edited_timestamp')
                    }).eq('message_id', msg['message_id']).execute()
                else:
                    # INSERT new message
                    clean_msg = {
                        'message_id': msg['message_id'],
                        'server_id': msg['server_id'],
                        'server_name': msg['server_name'],
                        'channel_id': msg['channel_id'],
                        'channel_name': msg['channel_name'],
                        'thread_name': msg.get('thread_name'),
                        'author_name': msg['author_name'],
                        'author_id': msg['author_id'],
                        'content': msg['content'],
                        'media_urls': msg['media_urls'],
                        'media_types': msg['media_types'],
                        'timestamp': msg['timestamp'],
                        'edited_timestamp': msg.get('edited_timestamp'),
                        'is_edited': msg['is_edited'],
                        'edit_count': 0,
                        'first_seen': datetime.now().isoformat(),
                        'last_seen': datetime.now().isoformat()
                    }
                    supabase.table('messages').insert(clean_msg).execute()
                    
            except Exception as e:
                print(f"    ⚠️ Error processing message {msg['message_id']}: {e}")
                
    except Exception as e:
        print(f"  ⚠️ Batch save failed: {e}")

def finalize_scan(active_message_ids):
    if not active_message_ids:
        return
    try:
        all_messages = supabase.table('messages').select('message_id').execute()
        if not all_messages.data:
            return
        to_remove = [m['message_id'] for m in all_messages.data if m['message_id'] not in active_message_ids]
        for msg_id in to_remove:
            supabase.table('messages').delete().eq('message_id', msg_id).execute()
        if to_remove:
            print(f"\n📦 Removed {len(to_remove)} messages not found")
    except:
        pass

# ========== SCANNER FUNCTIONS ==========

def scan_server(server):
    server_name = server['name']
    server_id = server['id']
    
    print(f"\n" + "=" * 60)
    print(f"🔍 SCANNING SERVER: {server_name}")
    print("=" * 60)
    
    channels = get_server_channels(server_id)
    if not channels:
        print(f"  No channels found")
        return [], []
    
    all_messages = []
    all_users = {}
    active_message_ids = []
    
    # PASS 1: Collect all messages and users
    print(f"\n📥 PASS 1: Collecting data...")
    for channel in channels:
        channel_name = channel['name']
        channel_id = channel['id']
        channel_type = channel['type']
        is_nsfw = channel.get('nsfw', False)
        
        if is_nsfw and not INCLUDE_NSFW:
            continue
        
        channel_matches = contains_keyword(channel_name)
        
        # Regular text channels and announcement channels
        if channel_type in [0, 5] and channel_matches:
            print(f"\n  📝 Scanning channel: #{channel_name}")
            if is_nsfw:
                confirm_nsfw(channel_id)
            
            messages = get_channel_messages(channel_id)
            print(f"    Found {len(messages)} messages")
            
            for msg in messages:
                processed = process_message(msg, server_name, server_id, channel_name, channel_id)
                
                if processed:
                    all_messages.append(processed)
                    active_message_ids.append(processed['message_id'])
                    
                    user_data = extract_user_from_message(msg)
                    if user_data and user_data['user_id'] not in all_users:
                        all_users[user_data['user_id']] = user_data
        
        # Forum channels
        elif channel_type == 15 and channel_matches:
            print(f"\n  📌 Scanning forum: #{channel_name}")
            if is_nsfw:
                confirm_nsfw(channel_id)
            
            threads = get_forum_threads(channel_id)
            print(f"    Found {len(threads)} total threads")
            
            for thread in threads:
                thread_name = thread.get('name', 'Unnamed')
                thread_id = thread['id']
                
                print(f"\n      📍 Thread: {thread_name[:50]}{'...' if len(thread_name) > 50 else ''}")
                
                # Get messages from this thread
                messages = get_channel_messages(thread_id)
                
                if messages:
                    print(f"        Found {len(messages)} messages")
                    
                    for msg in messages:
                        processed = process_message(msg, server_name, server_id, channel_name, channel_id, thread_name)
                        
                        if processed:
                            all_messages.append(processed)
                            active_message_ids.append(processed['message_id'])
                            
                            user_data = extract_user_from_message(msg)
                            if user_data and user_data['user_id'] not in all_users:
                                all_users[user_data['user_id']] = user_data
                else:
                    print(f"        No messages found")
    
    # PASS 2: Save/update users
    if all_users:
        print(f"\n👥 PASS 2: Processing {len(all_users)} users...")
        batch_save_users(all_users)
    
    # PASS 3: Save/update messages in batches
    if all_messages:
        messages_with_media = sum(1 for msg in all_messages if msg['media_urls'])
        print(f"\n📨 PASS 3: Processing {len(all_messages)} total messages ({messages_with_media} with media)")
        
        # Process in batches
        for i in range(0, len(all_messages), BATCH_SIZE):
            batch = all_messages[i:i+BATCH_SIZE]
            batch_save_messages(batch)
    
    print(f"\n📊 {server_name} - Found {len(all_messages)} messages, {len(all_users)} users")
    return all_messages, active_message_ids

def scan_all_servers():
    print("=" * 80)
    print("🔍 DISCORD ULTIMATE SCANNER - RENDER.COM VERSION")
    print("=" * 80)
    
    # Check if all required environment variables are set
    if not SUPABASE_URL or not SUPABASE_KEY or not TOKEN:
        print("❌ Missing environment variables!")
        print("   Make sure SUPABASE_URL, SUPABASE_KEY, and DISCORD_TOKEN are set in Render dashboard")
        return
    
    # Test token
    test_headers = get_headers()
    try:
        test_response = requests.get('https://discord.com/api/v9/users/@me', headers=test_headers)
        if test_response.status_code != 200:
            print(f"❌ Token invalid! Status: {test_response.status_code}")
            return
        print(f"✅ Token valid - logged in as: {test_response.json().get('username')}")
    except Exception as e:
        print(f"❌ Error testing token: {e}")
        return
    
    # Load ALL existing users into cache
    try:
        print(f"\n📦 Loading existing data into cache...")
        result = supabase.table('users').select('user_id').execute()
        for user in result.data:
            user_cache.add(user['user_id'])
        print(f"  👥 Loaded {len(user_cache)} existing users")
        
        # Load ALL existing message IDs into cache
        msg_result = supabase.table('messages').select('message_id').execute()
        for msg in msg_result.data:
            message_cache.add(msg['message_id'])
        print(f"  💬 Loaded {len(message_cache)} existing messages")
    except Exception as e:
        print(f"⚠️ Could not load cache: {e}")
    
    all_servers = get_all_servers()
    if not all_servers:
        print("❌ No servers found!")
        return
    
    if SCAN_ALL_SERVERS:
        servers_to_scan = all_servers
        print(f"\n📋 Found {len(servers_to_scan)} servers to scan")
    else:
        servers_to_scan = [s for s in all_servers if s['name'].lower() == TARGET_SERVER.lower()]
        if not servers_to_scan:
            print(f"❌ Server '{TARGET_SERVER}' not found!")
            return
        print(f"\n📋 Scanning single server: {TARGET_SERVER}")
    
    all_messages = []
    all_active_ids = []
    
    for server in servers_to_scan:
        messages, active_ids = scan_server(server)
        all_messages.extend(messages)
        all_active_ids.extend(active_ids)
        
        if len(servers_to_scan) > 1:
            print(f"\n⏳ Waiting between servers...")
            time.sleep(3)
    
    if all_active_ids:
        finalize_scan(all_active_ids)
    
    # Final statistics
    total_with_media = sum(1 for msg in all_messages if msg['media_urls'])
    total_spoilers = sum(1 for msg in all_messages for media_type in msg['media_types'] if media_type.startswith('spoiler_'))
    
    print(f"\n✅ SCAN COMPLETE!")
    print(f"   Total messages: {len(all_messages)}")
    print(f"   Messages with media: {total_with_media}")
    print(f"   Spoiler media items: {total_spoilers}")
    print(f"   Media percentage: {(total_with_media/len(all_messages)*100):.1f}%" if all_messages else "   0%")

if __name__ == "__main__":
    # This runs continuously on Render
    while True:
        try:
            scan_all_servers()
            print(f"\n⏳ Scan complete. Waiting 6 hours before next scan...")
            time.sleep(21600)  # Wait 6 hours before next scan
        except Exception as e:
            print(f"❌ Error in scan: {e}")
            print(f"⏳ Waiting 1 hour before retry...")
            time.sleep(3600)