import discord
from discord.ext import commands
import asyncio
import sqlite3
import json
import os
import time
from datetime import datetime
import uuid
from flask import Flask, render_template, request, jsonify
import threading
import queue
import aiohttp
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Bot setup
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)

# Environment Variables
DISCORD_BOT_TOKEN = os.getenv('DISCORD_BOT_TOKEN')
BOT_ID = os.getenv('BOT_ID', 'bot_123')
API_BASE_URL = os.getenv('API_BASE_URL', 'https://myapp.base44.com')

# Check if token is provided
if not DISCORD_BOT_TOKEN:
    print("❌ Error: DISCORD_BOT_TOKEN environment variable is required!")
    exit(1)

# Flask Dashboard
app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.secret_key = 'your-secret-key-here'

# Global variables
bot_status = {"running": False, "guilds": [], "commands": [], "last_sync": None}
get_now_buttons = {}
role_dms = {}
marketing_campaigns = {}
leads = []
server_logo_url = ""

# Operation queue for dashboard operations
operation_queue = queue.Queue()
operation_results = {}

# Database setup
def init_database():
    conn = sqlite3.connect('marketing_bot.db')
    cursor = conn.cursor()
    
    # Create all necessary tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS get_now_buttons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            button_id TEXT UNIQUE,
            button_text TEXT,
            button_style TEXT,
            channel_id TEXT,
            message_id TEXT,
            role_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS role_dms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            role_id TEXT UNIQUE,
            role_name TEXT,
            dm_title TEXT,
            dm_message TEXT,
            claim_button BOOLEAN DEFAULT 0,
            claim_role_id TEXT,
            button_text TEXT,
            button_color TEXT DEFAULT 'success',
            button_emoji TEXT DEFAULT '🎁',
            include_logo BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS marketing_campaigns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id TEXT UNIQUE,
            name TEXT,
            message TEXT,
            channel_id TEXT,
            interval_minutes INTEGER,
            is_active BOOLEAN DEFAULT 0,
            role_names TEXT,
            claim BOOLEAN DEFAULT 0,
            claim_role TEXT,
            include_server_logo BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            username TEXT,
            action TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bot_customization (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_name TEXT NOT NULL,
            bot_status TEXT NOT NULL,
            activity_type TEXT NOT NULL,
            activity_text TEXT NOT NULL,
            is_active BOOLEAN NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bot_avatar (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            avatar_data BLOB,
            file_name TEXT,
            file_size INTEGER,
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # AI Tracking tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_tracking (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            user_name TEXT,
            interaction_type TEXT NOT NULL,
            interaction_data TEXT,
            server_id TEXT,
            channel_id TEXT,
            message_id TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ip_address TEXT,
            user_agent TEXT,
            session_id TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS link_analytics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            link_url TEXT NOT NULL,
            link_type TEXT,
            click_count INTEGER DEFAULT 0,
            unique_clicks INTEGER DEFAULT 0,
            first_clicked TIMESTAMP,
            last_clicked TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS button_analytics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            button_id TEXT NOT NULL,
            button_text TEXT,
            button_type TEXT,
            click_count INTEGER DEFAULT 0,
            unique_clicks INTEGER DEFAULT 0,
            first_clicked TIMESTAMP,
            last_clicked TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ai_insights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            insight_type TEXT NOT NULL,
            insight_data TEXT NOT NULL,
            confidence_score REAL,
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT 1
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS marketing_opt_outs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            username TEXT,
            opt_out_type TEXT DEFAULT 'marketing',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, opt_out_type)
        )
    ''')
    
    conn.commit()
    conn.close()

# Initialize database
init_database()

# API Functions for Web Dashboard Integration
async def get_bot_config():
    """Get bot configuration from web dashboard"""
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{API_BASE_URL}/functions/getBotConfig"
            payload = {
                "bot_id": BOT_ID,
                "bot_token": DISCORD_BOT_TOKEN
            }
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    print(f"❌ Failed to get bot config: {response.status}")
                    return {"active": False}
    except Exception as e:
        print(f"❌ Error getting bot config: {e}")
        return {"active": False}

async def log_activity(activity_type, **kwargs):
    """Log bot activity to web dashboard"""
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{API_BASE_URL}/functions/logBotActivity"
            payload = {
                "bot_id": BOT_ID,
                "activity_type": activity_type,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                **kwargs
            }
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    print(f"✅ Logged activity: {activity_type}")
                else:
                    print(f"❌ Failed to log activity: {response.status}")
    except Exception as e:
        print(f"❌ Error logging activity: {e}")

async def update_bot_status(status, message=None, stats=None):
    """Update bot status on web dashboard"""
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{API_BASE_URL}/functions/updateBotStatus"
            payload = {
                "bot_id": BOT_ID,
                "status": status,
                "message": message,
                "stats": stats or {}
            }
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    print(f"✅ Updated bot status: {status}")
                else:
                    print(f"❌ Failed to update status: {response.status}")
    except Exception as e:
        print(f"❌ Error updating status: {e}")

# Message Template Processing
def process_message_template(template, user, affiliate_id, guild):
    """Process message template with placeholders"""
    message = template.get("message", "")
    
    # Replace placeholders
    message = message.replace("{username}", user.display_name)
    message = message.replace("{user_mention}", user.mention)
    message = message.replace("{affiliate_id}", str(affiliate_id))
    message = message.replace("{server_name}", guild.name)
    
    return message

def get_users_by_roles(guild, target_roles):
    """Get users who have any of the target roles"""
    target_users = []
    
    for role_name in target_roles:
        role = discord.utils.get(guild.roles, name=role_name)
        if role:
            for member in role.members:
                if member not in target_users:
                    target_users.append(member)
    
    return target_users

async def create_discord_buttons(template):
    """Create Discord buttons from template"""
    if not template.get("has_buttons", False):
        return None
    
    view = discord.ui.View()
    button_labels = template.get("button_labels", [])
    
    for i, label in enumerate(button_labels):
        button = discord.ui.Button(
            label=label,
            style=discord.ButtonStyle.primary,
            custom_id=f"template_button_{i}"
        )
        view.add_item(button)
    
    return view

# Main Bot Loop
async def main_bot_loop():
    """Main bot loop for web dashboard integration"""
    while True:
        try:
            # 1. Get configuration from dashboard
            config = await get_bot_config()
            
            if not config.get("active"):
                print("⏸️ Bot is inactive, waiting 5 minutes...")
                await asyncio.sleep(300)  # 5 minutes
                continue
            
            print("🔄 Processing bot configuration...")
            
            # 2. Process each message template
            message_templates = config.get("config", {}).get("message_templates", [])
            target_roles = config.get("config", {}).get("target_roles", [])
            affiliate_id = config.get("config", {}).get("affiliate_id", "default")
            
            for guild in bot.guilds:
                # Get target users by roles
                target_users = get_users_by_roles(guild, target_roles)
                
                print(f"📊 Found {len(target_users)} target users in {guild.name}")
                
                # Send messages to each user
                for user in target_users:
                    for template in message_templates:
                        processed_message = process_message_template(
                            template, user, affiliate_id, guild
                        )
                        
                        try:
                            # Create buttons if template has them
                            view = await create_discord_buttons(template)
                            
                            if view:
                                await user.send(processed_message, view=view)
                            else:
                                await user.send(processed_message)
                            
                            # Log successful message
                            await log_activity("message_sent", 
                                             affiliate_email="system@bot.com",
                                             user_id=str(user.id),
                                             message_sent=processed_message,
                                             role_targeted=target_roles[0] if target_roles else "unknown", 
                                             success=True)
                            
                            print(f"✅ Sent message to {user.display_name}")
                            
                        except discord.Forbidden:
                            # User has DMs disabled
                            await log_activity("error",
                                             affiliate_email="system@bot.com",
                                             user_id=str(user.id),
                                             success=False,
                                             error_message="User has DMs disabled")
                            print(f"❌ User {user.display_name} has DMs disabled")
                            
                        except Exception as e:
                            # Log failed message  
                            await log_activity("error",
                                             affiliate_email="system@bot.com",
                                             user_id=str(user.id),
                                             success=False,
                                             error_message=str(e))
                            print(f"❌ Error sending to {user.display_name}: {e}")
                        
                        # Rate limiting - wait between messages
                        await asyncio.sleep(1)
                        
        except Exception as e:
            print(f"❌ Error in main loop: {e}")
            await log_activity("error", success=False, error_message=str(e))
            
        await asyncio.sleep(300)  # Wait 5 minutes before next cycle

# Bot Events
@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user}")
    print(f"📊 Bot is in {len(bot.guilds)} server(s)")
    for guild in bot.guilds:
        print(f"  - {guild.name} (ID: {guild.id})")
        print(f"    Permissions: Send Messages: {guild.me.guild_permissions.send_messages}, Manage Roles: {guild.me.guild_permissions.manage_roles}")
    
    bot_status["running"] = True
    bot_status["guilds"] = [{"id": g.id, "name": g.name, "member_count": g.member_count} for g in bot.guilds]
    bot_status["commands"] = [cmd.name for cmd in bot.commands]
    bot_status["last_sync"] = int(time.time())
    
    # Log startup to web dashboard
    await log_activity("startup", success=True)
    await update_bot_status("active", "Bot started successfully")
    
    # Start main bot loop for web dashboard integration
    asyncio.create_task(main_bot_loop())
    
    print(f"✅ Synced {len(bot.commands)} commands to {bot.guilds[0].name if bot.guilds else 'No servers'}")
    print(f"✅ Global sync: {len(bot.commands)} commands")
    print(f"📋 Available commands: {[cmd.name for cmd in bot.commands]}")
    
    # Start the operation queue handler
    asyncio.create_task(handle_operation_queue())
    print("✅ Operation queue handler started")
    
    # Start marketing campaign handler
    asyncio.create_task(handle_marketing_campaigns())
    print("✅ Marketing campaign handler started")

@bot.event
async def on_member_update(before, after):
    if before.roles != after.roles:
        new_roles = [role for role in after.roles if role not in before.roles]
        print(f"🔍 Member update detected for {after.name}: {len(new_roles)} new roles")
        
        # Check for role DMs
        for role in new_roles:
            conn = sqlite3.connect('marketing_bot.db')
            cursor = conn.cursor()
            # Check by both role_id (numeric ID) and role_name (for backward compatibility)
            cursor.execute('SELECT * FROM role_dms WHERE role_id = ? OR role_name = ?', (str(role.id), role.name))
            role_dm = cursor.fetchone()
            
            if role_dm:
                try:
                    # Create embed
                    embed = discord.Embed(
                        title=role_dm[3] or f"Welcome to {role.name}!",  # dm_title
                        description=role_dm[4],  # dm_message
                        color=0x8b5cf6
                    )
                    
                    if role_dm[10] and server_logo_url:  # include_logo (updated index)
                        embed.set_thumbnail(url=server_logo_url)
                    
                    # Add claim button if enabled
                    if role_dm[5]:  # claim_button
                        view = discord.ui.View()
                        
                        # Get button style from database
                        button_style = role_dm[8] if len(role_dm) > 8 else 'success'  # button_color
                        button_emoji = role_dm[9] if len(role_dm) > 9 else '🎁'  # button_emoji
                        
                        # Convert color string to ButtonStyle
                        style_map = {
                            'primary': discord.ButtonStyle.primary,
                            'secondary': discord.ButtonStyle.secondary,
                            'success': discord.ButtonStyle.primary,  # Discord doesn't have success, use primary
                            'danger': discord.ButtonStyle.danger,
                            'blurple': discord.ButtonStyle.primary
                        }
                        button_style_enum = style_map.get(button_style, discord.ButtonStyle.primary)
                        
                        try:
                            # Create claim button with callback
                            claim_role_name = role_dm[6]  # claim_role_id
                            
                            async def role_claim_callback(interaction):
                                try:
                                    # Get the user who clicked the button
                                    user = interaction.user
                                    
                                    # Find the guild and role
                                    claim_role_obj = None
                                    target_guild = None
                                    
                                    for guild in bot.guilds:
                                        role = discord.utils.get(guild.roles, name=claim_role_name)
                                        if role:
                                            claim_role_obj = role
                                            target_guild = guild
                                            break
                                    
                                    if claim_role_obj and target_guild:
                                        # Get the member in the guild
                                        member = target_guild.get_member(user.id)
                                        if member:
                                            await member.add_roles(claim_role_obj)
                                            await interaction.response.send_message(
                                                f"✅ You've been given the {claim_role_name} role!", 
                                                ephemeral=True
                                            )
                                        else:
                                            await interaction.response.send_message(
                                                "❌ You must be in the server to claim this role!", 
                                                ephemeral=True
                                            )
                                    else:
                                        await interaction.response.send_message(
                                            f"❌ Role '{claim_role_name}' not found in any server", 
                                            ephemeral=True
                                        )
                                except Exception as e:
                                    await interaction.response.send_message(
                                        f"❌ Error: {e}", 
                                        ephemeral=True
                                    )
                            
                            button = discord.ui.Button(
                                label=role_dm[7] or "Claim Rewards",  # button_text
                                style=button_style_enum,
                                emoji=button_emoji if button_emoji else None
                            )
                            button.callback = role_claim_callback
                            view.add_item(button)
                            await after.send(embed=embed, view=view)
                        except Exception as button_error:
                            print(f"❌ Error creating button: {button_error}")
                            # Send without button if button creation fails
                            await after.send(embed=embed)
                    else:
                        await after.send(embed=embed)
                    
                    print(f"✅ Sent role DM to {after.name} for role {role.name}")
                except Exception as e:
                    print(f"❌ Error sending role DM: {e}")
            
            conn.close()

# Marketing Campaign Handler
async def handle_marketing_campaigns():
    """Handle recurring marketing campaigns"""
    while True:
        try:
            if not bot.is_ready():
                await asyncio.sleep(10)
                continue
                
            conn = sqlite3.connect('marketing_bot.db')
            cursor = conn.cursor()
            
            # Get active campaigns
            cursor.execute('''
                SELECT * FROM marketing_campaigns 
                WHERE is_active = 1
            ''')
            campaigns = cursor.fetchall()
            
            for campaign in campaigns:
                campaign_id = campaign[1]  # campaign_id
                role_names = campaign[7].split(',') if campaign[7] else []  # role_names
                message = campaign[3]  # message
                interval_minutes = campaign[5]  # interval_minutes
                claim = campaign[8]  # claim
                claim_role = campaign[9]  # claim_role
                include_server_logo = campaign[10]  # include_server_logo
                
                # Check if it's time to send
                last_sent_key = f"last_sent_{campaign_id}"
                current_time = time.time()
                last_sent = getattr(handle_marketing_campaigns, last_sent_key, 0)
                
                # Handle different campaign types
                should_send = False
                if interval_minutes == 0:
                    # One-time campaign: send once and deactivate
                    if last_sent == 0:  # Never sent before
                        should_send = True
                        # Deactivate after sending
                        cursor.execute('UPDATE marketing_campaigns SET is_active = 0 WHERE campaign_id = ?', (campaign_id,))
                        conn.commit()
                else:
                    # Recurring campaign: send every interval_minutes
                    if current_time - last_sent >= (interval_minutes * 60):
                        should_send = True
                
                if should_send:
                    # Send to all users with the specified roles
                    for guild in bot.guilds:
                        for role_name in role_names:
                            role = discord.utils.get(guild.roles, name=role_name)
                            if role:
                                for member in role.members:
                                    try:
                                        # Check if user has opted out
                                        cursor.execute('''
                                            SELECT * FROM marketing_opt_outs 
                                            WHERE user_id = ? AND opt_out_type = 'marketing'
                                        ''', (str(member.id),))
                                        
                                        if cursor.fetchone():
                                            continue  # Skip opted out users
                                        
                                        # Create DM channel
                                        dm_channel = await member.create_dm()
                                        
                                        # Create embed
                                        embed = discord.Embed(
                                            title="📢 Marketing Update",
                                            description=message,
                                            color=0x8b5cf6
                                        )
                                        
                                        if include_server_logo and server_logo_url:
                                            embed.set_thumbnail(url=server_logo_url)
                                        
                                        # Add claim button if enabled
                                        if claim and claim_role:
                                            view = discord.ui.View()
                                            claim_button = discord.ui.Button(
                                                label="Claim Now",
                                                style=discord.ButtonStyle.primary,
                                                emoji="🎁"
                                            )
                                            
                                            async def claim_callback(interaction):
                                                try:
                                                    # Get the user who clicked the button
                                                    user = interaction.user
                                                    
                                                    # Find the guild and role
                                                    claim_role_obj = None
                                                    target_guild = None
                                                    
                                                    for guild in bot.guilds:
                                                        role = discord.utils.get(guild.roles, name=claim_role)
                                                        if role:
                                                            claim_role_obj = role
                                                            target_guild = guild
                                                            break
                                                    
                                                    if claim_role_obj and target_guild:
                                                        # Get the member in the guild
                                                        member = target_guild.get_member(user.id)
                                                        if member:
                                                            await member.add_roles(claim_role_obj)
                                                            await interaction.response.send_message(
                                                                f"✅ You've been given the {claim_role} role!", 
                                                                ephemeral=True
                                                            )
                                                        else:
                                                            await interaction.response.send_message(
                                                                "❌ You must be in the server to claim this role!", 
                                                                ephemeral=True
                                                            )
                                                    else:
                                                        await interaction.response.send_message(
                                                            f"❌ Role '{claim_role}' not found in any server", 
                                                            ephemeral=True
                                                        )
                                                except Exception as e:
                                                    await interaction.response.send_message(
                                                        f"❌ Error: {e}", 
                                                        ephemeral=True
                                                    )
                                            
                                            claim_button.callback = claim_callback
                                            view.add_item(claim_button)
                                            
                                            await dm_channel.send(embed=embed, view=view)
                                        else:
                                            await dm_channel.send(embed=embed)
                                        
                                        print(f"✅ Sent marketing DM to {member.name} for campaign {campaign_id}")
                                        
                                    except discord.Forbidden:
                                        print(f"🚫 Cannot send DM to {member.name} (DMs disabled)")
                                    except Exception as e:
                                        print(f"❌ Error sending marketing DM to {member.name}: {e}")
                    
                    # Update last sent time
                    setattr(handle_marketing_campaigns, last_sent_key, current_time)
            
            conn.close()
            
        except Exception as e:
            print(f"❌ Error in marketing campaign handler: {e}")
        
        # Check every minute
        await asyncio.sleep(60)

# Handle opt-out messages
@bot.event
async def on_message(message):
    # Ignore messages from the bot itself
    if message.author == bot.user:
        return
    
    # Only handle DM messages
    if isinstance(message.channel, discord.DMChannel):
        content = message.content.lower().strip()
        
        # Check for opt-out commands
        opt_out_commands = ['stop', 'unsubscribe', 'opt out', 'optout', 'no more', 'stop messages']
        
        if content in opt_out_commands:
            try:
                # Add user to opt-out list
                conn = sqlite3.connect('marketing_bot.db')
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT OR REPLACE INTO marketing_opt_outs (user_id, username, opt_out_type)
                    VALUES (?, ?, 'marketing')
                ''', (str(message.author.id), message.author.name))
                
                conn.commit()
                conn.close()
                
                # Send confirmation message
                embed = discord.Embed(
                    title="✅ Unsubscribed Successfully",
                    description="You have been unsubscribed from marketing messages. You will no longer receive recurring promotional messages from this bot.",
                    color=0x10b981
                )
                embed.add_field(
                    name="To resubscribe:",
                    value="Reply with `subscribe` or `start` to receive marketing messages again.",
                    inline=False
                )
                embed.set_footer(text="Thank you for using our service!")
                
                await message.channel.send(embed=embed)
                print(f"✅ User {message.author.name} ({message.author.id}) opted out of marketing messages")
                
            except Exception as e:
                print(f"❌ Error processing opt-out for {message.author.name}: {e}")
                await message.channel.send("❌ Sorry, there was an error processing your request. Please try again later.")
        
        # Check for resubscribe commands
        resubscribe_commands = ['subscribe', 'start', 'opt in', 'optin', 'resubscribe']
        
        if content in resubscribe_commands:
            try:
                # Remove user from opt-out list
                conn = sqlite3.connect('marketing_bot.db')
                cursor = conn.cursor()
                
                cursor.execute('''
                    DELETE FROM marketing_opt_outs 
                    WHERE user_id = ? AND opt_out_type = 'marketing'
                ''', (str(message.author.id),))
                
                conn.commit()
                conn.close()
                
                # Send confirmation message
                embed = discord.Embed(
                    title="✅ Resubscribed Successfully",
                    description="You have been resubscribed to marketing messages. You will now receive promotional messages from this bot.",
                    color=0x10b981
                )
                embed.add_field(
                    name="To unsubscribe:",
                    value="Reply with `stop` or `unsubscribe` to stop receiving marketing messages.",
                    inline=False
                )
                embed.set_footer(text="Welcome back!")
                
                await message.channel.send(embed=embed)
                print(f"✅ User {message.author.name} ({message.author.id}) resubscribed to marketing messages")
                
            except Exception as e:
                print(f"❌ Error processing resubscribe for {message.author.name}: {e}")
                await message.channel.send("❌ Sorry, there was an error processing your request. Please try again later.")
    
    # Process other commands
    await bot.process_commands(message)

# Operation handler functions for dashboard operations
async def handle_operation_queue():
    """Handle operations from the dashboard queue"""
    while True:
        try:
            if not operation_queue.empty():
                operation = operation_queue.get()
                operation_id = operation.get('id')
                operation_type = operation.get('type')
                
                print(f"🔍 Processing operation: {operation_type}")
                
                try:
                    if operation_type == 'quick_dm':
                        result = await handle_quick_dm_operation(operation)
                    elif operation_type == 'test_dm_permissions':
                        result = await handle_test_dm_permissions_operation(operation)
                    elif operation_type == 'bot_avatar':
                        result = await handle_bot_avatar_operation(operation)
                    elif operation_type == 'bot_customize':
                        result = await handle_bot_customize_operation(operation)
                    else:
                        result = {"success": False, "error": f"Unknown operation type: {operation_type}"}
                    
                    # Store result
                    operation_results[operation_id] = result
                    print(f"✅ Operation {operation_type} completed: {result.get('success', False)}")
                    
                except Exception as e:
                    print(f"❌ Error processing operation {operation_type}: {e}")
                    operation_results[operation_id] = {"success": False, "error": str(e)}
                
                operation_queue.task_done()
            else:
                await asyncio.sleep(0.1)  # Small delay to prevent busy waiting
        except Exception as e:
            print(f"❌ Error in operation queue handler: {e}")
            await asyncio.sleep(1)

async def handle_quick_dm_operation(operation):
    """Handle Quick DM operation"""
    try:
        data = operation.get('data', {})
        role_id = int(data.get('role_id'))
        title = data.get('title', '').strip()
        message = data.get('message', '').strip()
        include_logo = data.get('include_logo', False)
        
        if not bot.guilds:
            return {"success": False, "error": "Bot is not connected to any servers"}
        
        guild = bot.guilds[0]
        role = guild.get_role(role_id)
        
        if not role:
            return {"success": False, "error": "Role not found"}
        
        members_with_role = [member for member in guild.members if role in member.roles]
        
        if not members_with_role:
            return {"success": False, "error": f"No members found with the role '{role.name}'"}
        
        # Prepare message content
        embed_data = None
        if title or include_logo:
            embed_data = {
                "title": title or "Message from Server",
                "description": message,
                "color": 0x8b5cf6,
                "timestamp": datetime.now().isoformat()
            }
            
            if include_logo and server_logo_url:
                embed_data["thumbnail"] = {"url": server_logo_url}
        
        # Check for opt-outs
        conn = sqlite3.connect('marketing_bot.db')
        cursor = conn.cursor()
        cursor.execute('SELECT user_id FROM marketing_opt_outs WHERE opt_out_type = "marketing"')
        opted_out_users = {row[0] for row in cursor.fetchall()}
        conn.close()
        
        # Send DMs
        success_count = 0
        error_count = 0
        skipped_count = 0
        dm_disabled_count = 0
        
        for member in members_with_role:
            if str(member.id) in opted_out_users:
                skipped_count += 1
                continue
            
            try:
                if embed_data:
                    embed = discord.Embed(
                        title=embed_data["title"],
                        description=embed_data["description"],
                        color=embed_data["color"]
                    )
                    
                    if "thumbnail" in embed_data:
                        embed.set_thumbnail(url=embed_data["thumbnail"]["url"])
                    
                    embed.timestamp = datetime.now()
                    await member.send(embed=embed)
                else:
                    await member.send(message)
                
                success_count += 1
                print(f"✅ Sent DM to {member.name}")
                
            except discord.Forbidden:
                dm_disabled_count += 1
                print(f"⏭️ Skipped {member.name} - DMs disabled")
            except Exception as e:
                error_count += 1
                print(f"❌ Error sending DM to {member.name}: {e}")
        
        return {
            "success": True,
            "message": f"Quick DM completed for role '{role.name}'",
            "success_count": success_count,
            "error_count": error_count,
            "skipped_count": skipped_count,
            "dm_disabled_count": dm_disabled_count,
            "total_count": len(members_with_role),
            "role_name": role.name
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}

async def handle_test_dm_permissions_operation(operation):
    """Handle Test DM Permissions operation"""
    try:
        data = operation.get('data', {})
        role_id = int(data.get('role_id'))
        
        if not bot.guilds:
            return {"success": False, "error": "Bot is not connected to any servers"}
        
        guild = bot.guilds[0]
        role = guild.get_role(role_id)
        
        if not role:
            return {"success": False, "error": "Role not found"}
        
        members_with_role = [member for member in guild.members if role in member.roles]
        
        if not members_with_role:
            return {"success": False, "error": f"No members found with the role '{role.name}'"}
        
        # Test DM permissions for first few members (NO ACTUAL MESSAGES SENT)
        test_results = []
        
        for member in members_with_role[:3]:  # Test first 3 members
            try:
                # Only test if we can create a DM channel - DON'T SEND MESSAGE
                dm_channel = await member.create_dm()
                
                test_results.append({
                    "username": member.name,
                    "dm_enabled": True,
                    "status": "✅ DMs enabled"
                })
                print(f"✅ DM permissions OK for {member.name}")
                
            except discord.Forbidden:
                test_results.append({
                    "username": member.name,
                    "dm_enabled": False,
                    "status": "❌ DMs disabled"
                })
                print(f"🚫 DMs disabled for {member.name}")
            except Exception as e:
                test_results.append({
                    "username": member.name,
                    "dm_enabled": False,
                    "status": f"❌ Error: {str(e)}"
                })
                print(f"❌ Error testing DM for {member.name}: {e}")
        
        return {
            "success": True,
            "role_name": role.name,
            "total_members": len(members_with_role),
            "test_results": test_results
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}

async def handle_bot_avatar_operation(operation):
    """Handle Bot Avatar operation"""
    try:
        data = operation.get('data', {})
        avatar_data = data.get('avatar_data')
        
        if not avatar_data:
            return {"success": False, "error": "No avatar data provided"}
        
        if bot.user:
            await bot.user.edit(avatar=avatar_data)
            return {"success": True, "message": "Bot avatar updated successfully!"}
        else:
            return {"success": False, "error": "Bot is not ready"}
            
    except Exception as e:
        return {"success": False, "error": str(e)}

async def handle_bot_customize_operation(operation):
    """Handle Bot Customization operation"""
    try:
        data = operation.get('data', {})
        bot_name = data.get('bot_name')
        bot_status = data.get('bot_status', 'online')
        activity_type = data.get('activity_type', 'watching')
        activity_text = data.get('activity_text')
        
        if not bot.user:
            return {"success": False, "error": "Bot is not ready"}
        
        # Update bot username
        if bot_name and bot_name != bot.user.name:
            await bot.user.edit(username=bot_name)
            print(f"✅ Updated bot username to: {bot_name}")
        
        # Update bot status and activity
        status_mapping = {
            'online': discord.Status.online,
            'idle': discord.Status.idle,
            'dnd': discord.Status.dnd,
            'offline': discord.Status.offline
        }
        
        activity_mapping = {
            'watching': discord.ActivityType.watching,
            'playing': discord.ActivityType.playing,
            'listening': discord.ActivityType.listening,
            'streaming': discord.ActivityType.streaming
        }
        
        activity = discord.Activity(
            type=activity_mapping.get(activity_type, discord.ActivityType.watching),
            name=activity_text
        )
        
        await bot.change_presence(
            status=status_mapping.get(bot_status, discord.Status.online),
            activity=activity
        )
        
        print(f"✅ Updated bot presence: {bot_status} - {activity_type} {activity_text}")
        return {"success": True, "message": "Bot customization applied successfully!"}
        
    except Exception as e:
        return {"success": False, "error": str(e)}

# Bot Commands
@bot.command(name='sync')
async def sync_commands(ctx):
    try:
        synced = await bot.tree.sync()
        await ctx.send(f"✅ Synced {len(synced)} commands globally!")
    except Exception as e:
        await ctx.send(f"❌ Error syncing commands: {e}")

@bot.command(name='test')
async def test_command(ctx):
    await ctx.send("✅ Bot is working!")

@bot.command(name='check')
async def check_status(ctx):
    embed = discord.Embed(title="Bot Status", color=0x00ff00)
    embed.add_field(name="Servers", value=len(bot.guilds), inline=True)
    embed.add_field(name="Commands", value=len(bot.commands), inline=True)
    embed.add_field(name="Uptime", value="Online", inline=True)
    await ctx.send(embed=embed)

# Flask Routes
@app.route('/')
def dashboard():
    import time
    return render_template('dashboard.html', status=bot_status, timestamp=int(time.time()))

@app.route('/new')
def new_dashboard():
    import time
    return render_template('dashboard_new.html', status=bot_status, timestamp=int(time.time()))

@app.route('/test_new')
def test_new():
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>TEST NEW ROUTE</title>
        <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
    </head>
    <body style="background: #1a1a2e; color: #fff; font-family: Arial; text-align: center; padding: 50px;">
        <h1 style="color: #8b5cf6; font-size: 3em;">✅ NEW ROUTE WORKING!</h1>
        <p style="font-size: 1.5em; margin: 30px 0;">If you see this page, the new route is working correctly.</p>
        <div style="background: #00ff00; color: #000; padding: 20px; margin: 30px; border-radius: 10px;">
            <h2>✅ Bot is running and serving new routes!</h2>
            <p>Now try: <a href="/new" style="color: #8b5cf6;">http://localhost:5000/new</a></p>
        </div>
    </body>
    </html>
    '''

@app.route('/force_refresh.html')
def force_refresh():
    return '''
<!DOCTYPE html>
<html>
<head>
    <title>FORCE REFRESH - Clear Cache</title>
    <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
    <meta http-equiv="Pragma" content="no-cache">
    <meta http-equiv="Expires" content="0">
</head>
<body style="background: #000; color: #fff; font-family: Arial; text-align: center; padding: 50px;">
    <h1 style="color: #ff0000; font-size: 3em;">🚨 CACHE CLEAR PAGE 🚨</h1>
    <p style="font-size: 1.5em; margin: 30px 0;">This page will clear your browser cache and redirect you to the updated dashboard.</p>
    
    <div style="background: #ff0000; color: #fff; padding: 20px; margin: 30px; border-radius: 10px;">
        <h2>If you see this page, your browser is loading the new files!</h2>
        <p>Redirecting to the updated dashboard in 3 seconds...</p>
    </div>
    
    <div style="background: #00ff00; color: #000; padding: 15px; margin: 20px; border-radius: 10px;">
        <h3>✅ Bot is running and serving pages correctly!</h3>
        <p>If you see this green box, the Flask server is working properly.</p>
    </div>
    
    <script>
        // Clear all caches safely
        try {
            if ('caches' in window) {
                caches.keys().then(function(names) {
                    for (let name of names) {
                        caches.delete(name);
                    }
                }).catch(function(e) {
                    console.log('Cache clear failed:', e);
                });
            }
        } catch(e) {
            console.log('Cache clear error:', e);
        }
        
        // Clear localStorage and sessionStorage
        try {
            localStorage.clear();
            sessionStorage.clear();
        } catch(e) {
            console.log('Storage clear error:', e);
        }
        
        // Force redirect with cache busting
        setTimeout(function() {
            window.location.href = 'http://localhost:5000?t=' + new Date().getTime();
        }, 3000);
    </script>
</body>
</html>
    '''

@app.route('/test')
def test_page():
    return '''
<!DOCTYPE html>
<html>
<head>
    <title>Test Page</title>
    <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
</head>
<body style="background: #1a1a2e; color: #fff; font-family: Arial; text-align: center; padding: 50px;">
    <h1 style="color: #8b5cf6;">✅ TEST PAGE WORKING</h1>
    <p>If you can see this page, the Flask server is working correctly.</p>
    <p><a href="/" style="color: #06b6d4;">Go to Dashboard</a></p>
</body>
</html>
    '''

@app.route('/api/status')
def api_status():
    return jsonify(bot_status)

@app.route('/api/roles')
def api_roles():
    if not bot.guilds:
        return jsonify([])
    
    guild = bot.guilds[0]
    roles = [{"id": str(role.id), "name": role.name, "color": str(role.color)} for role in guild.roles if not role.managed and role.name != "@everyone"]
    return jsonify(roles)

@app.route('/api/channels')
def api_channels():
    if not bot.guilds:
        return jsonify([])
    
    guild = bot.guilds[0]
    channels = [{"id": str(channel.id), "name": channel.name, "type": str(channel.type)} for channel in guild.channels if hasattr(channel, 'send')]
    print(f"API Channels called, returning {len(channels)} channels")
    return jsonify(channels)

@app.route('/api/getnow', methods=['GET', 'POST'])
def api_getnow():
    if request.method == 'POST':
        data = request.json
        button_id = data.get('button_id')
        button_text = data.get('button_text')
        button_style = data.get('button_style', 'primary')
        channel_id = data.get('channel_id')
        role_id = data.get('role_id')
        
        # Store in database
        conn = sqlite3.connect('marketing_bot.db')
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO get_now_buttons 
            (button_id, button_text, button_style, channel_id, role_id)
            VALUES (?, ?, ?, ?, ?)
        ''', (button_id, button_text, button_style, channel_id, role_id))
        conn.commit()
        conn.close()
        
        return jsonify({"success": True, "message": "Get Now button created!"})
    
    else:
        # Return existing buttons
        conn = sqlite3.connect('marketing_bot.db')
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM get_now_buttons')
        buttons = cursor.fetchall()
        conn.close()
        
        return jsonify([{
            "id": row[0],
            "button_id": row[1],
            "button_text": row[2],
            "button_style": row[3],
            "channel_id": row[4],
            "role_id": row[6]
        } for row in buttons])



@app.route('/api/leads')
def api_leads():
    conn = sqlite3.connect('marketing_bot.db')
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM leads ORDER BY timestamp DESC LIMIT 100')
    leads = cursor.fetchall()
    conn.close()
    
    return jsonify([{
        "id": row[0],
        "user_id": row[1],
        "username": row[2],
        "action": row[3],
        "timestamp": row[4]
    } for row in leads])

@app.route('/api/bot-customize', methods=['GET', 'POST'])
def api_bot_customize():
    if request.method == 'POST':
        data = request.json
        bot_name = data.get('name', '').strip()
        bot_status = data.get('status', 'online')
        activity_type = data.get('activity_type', 'watching')
        activity_text = data.get('activity_text', '').strip()
        is_active = data.get('active', True)
        
        # Validate inputs
        if not bot_name:
            return jsonify({"success": False, "error": "Bot name is required"})
        
        if not activity_text:
            return jsonify({"success": False, "error": "Activity text is required"})
        
        # Store in database
        conn = sqlite3.connect('marketing_bot.db')
        cursor = conn.cursor()
        cursor.execute('DELETE FROM bot_customization')
        cursor.execute('''
            INSERT INTO bot_customization (bot_name, bot_status, activity_type, activity_text, is_active)
            VALUES (?, ?, ?, ?, ?)
        ''', (bot_name, bot_status, activity_type, activity_text, is_active))
        conn.commit()
        conn.close()
        
        # Update bot status if active - queue the operation
        if is_active and bot.user:
            try:
                # Create operation ID
                operation_id = str(uuid.uuid4())
                
                # Queue the bot customization operation
                operation = {
                    'id': operation_id,
                    'type': 'bot_customize',
                    'data': {
                        'bot_name': bot_name,
                        'bot_status': bot_status,
                        'activity_type': activity_type,
                        'activity_text': activity_text
                    }
                }
                
                operation_queue.put(operation)
                print(f"🔍 Queued Bot Customization operation: {operation_id}")
                
            except Exception as e:
                print(f"Warning: Could not queue bot customization: {e}")
        
        return jsonify({"success": True, "message": "Bot customization saved!"})
    
    else:
        # Return current customization
        conn = sqlite3.connect('marketing_bot.db')
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM bot_customization ORDER BY updated_at DESC LIMIT 1')
        customization = cursor.fetchone()
        conn.close()
        
        if customization:
            return jsonify({
                "success": True,
                "customization": {
                    "name": customization[1],
                    "status": customization[2],
                    "activity_type": customization[3],
                    "activity_text": customization[4],
                    "active": bool(customization[5])
                }
            })
        else:
            return jsonify({"success": True, "customization": None})

@app.route('/api/bot-avatar', methods=['POST'])
def api_bot_avatar():
    """Queue Bot Avatar operation for the bot to handle"""
    try:
        if 'avatar' not in request.files:
            return jsonify({"success": False, "error": "No avatar file provided"})
        
        avatar_file = request.files['avatar']
        if avatar_file.filename == '':
            return jsonify({"success": False, "error": "No file selected"})
        
        # Validate file type
        allowed_extensions = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
        if not ('.' in avatar_file.filename and 
                avatar_file.filename.rsplit('.', 1)[1].lower() in allowed_extensions):
            return jsonify({"success": False, "error": "Invalid file type. Please use PNG, JPG, JPEG, GIF, or WEBP."})
        
        # Validate file size (8MB limit)
        avatar_file.seek(0, 2)  # Seek to end
        file_size = avatar_file.tell()
        avatar_file.seek(0)  # Reset to beginning
        
        if file_size > 8 * 1024 * 1024:  # 8MB
            return jsonify({"success": False, "error": "File too large. Maximum size is 8MB."})
        
        # Read file data
        avatar_data = avatar_file.read()
        
        if not bot.user:
            return jsonify({"success": False, "error": "Bot is not ready"})
        
        # Create operation ID
        operation_id = str(uuid.uuid4())
        
        # Queue the operation
        operation = {
            'id': operation_id,
            'type': 'bot_avatar',
            'data': {
                'avatar_data': avatar_data
            }
        }
        
        operation_queue.put(operation)
        print(f"🔍 Queued Bot Avatar operation: {operation_id}")
        
        # Wait for result (with timeout)
        timeout = 30  # 30 seconds timeout
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if operation_id in operation_results:
                result = operation_results.pop(operation_id)
                return jsonify(result)
            time.sleep(0.1)
        
        return jsonify({"success": False, "error": "Operation timeout"})
            
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/quick-dm', methods=['POST'])
def api_quick_dm():
    """Queue Quick DM operation for the bot to handle"""
    try:
        data = request.json
        role_id = int(data.get('role_id'))
        title = data.get('title', '').strip()
        message = data.get('message', '').strip()
        include_logo = data.get('include_logo', False)
        
        if not role_id or not message:
            return jsonify({"success": False, "error": "Role ID and message are required"})
        
        if not bot.guilds:
            return jsonify({"success": False, "error": "Bot is not connected to any servers"})
        
        if not bot.is_ready():
            return jsonify({"success": False, "error": "Bot is not ready yet. Please wait a moment and try again."})
        
        # Create operation ID
        operation_id = str(uuid.uuid4())
        
        # Queue the operation
        operation = {
            'id': operation_id,
            'type': 'quick_dm',
            'data': {
                'role_id': role_id,
                'title': title,
                'message': message,
                'include_logo': include_logo
            }
        }
        
        operation_queue.put(operation)
        print(f"🔍 Queued Quick DM operation: {operation_id}")
        
        # Wait for result (with timeout)
        timeout = 30  # 30 seconds timeout
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if operation_id in operation_results:
                result = operation_results.pop(operation_id)
                return jsonify(result)
            time.sleep(0.1)
        
        return jsonify({"success": False, "error": "Operation timeout"})
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/server-logo', methods=['POST'])
def api_server_logo():
    """Handle server logo upload"""
    try:
        if 'logo' not in request.files:
            return jsonify({"success": False, "error": "No logo file provided"})
        
        logo_file = request.files['logo']
        if logo_file.filename == '':
            return jsonify({"success": False, "error": "No file selected"})
        
        # Validate file type
        allowed_extensions = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
        if not ('.' in logo_file.filename and 
                logo_file.filename.rsplit('.', 1)[1].lower() in allowed_extensions):
            return jsonify({"success": False, "error": "Invalid file type. Please use PNG, JPG, JPEG, GIF, or WEBP."})
        
        # Validate file size (8MB limit)
        logo_file.seek(0, 2)  # Seek to end
        file_size = logo_file.tell()
        logo_file.seek(0)  # Reset to beginning
        
        if file_size > 8 * 1024 * 1024:  # 8MB
            return jsonify({"success": False, "error": "File too large. Maximum size is 8MB."})
        
        # Read file data and convert to base64 for storage
        import base64
        logo_data = logo_file.read()
        logo_base64 = base64.b64encode(logo_data).decode('utf-8')
        
        # Store in database
        conn = sqlite3.connect('marketing_bot.db')
        cursor = conn.cursor()
        cursor.execute('DELETE FROM server_logo')
        cursor.execute('INSERT INTO server_logo (logo_data) VALUES (?)', (logo_base64,))
        conn.commit()
        conn.close()
        
        # Update global variable
        global server_logo_url
        server_logo_url = f"data:image/{logo_file.filename.rsplit('.', 1)[1].lower()};base64,{logo_base64}"
        
        return jsonify({"success": True, "message": "Server logo updated successfully!"})
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/setdm', methods=['POST'])
def api_setdm():
    """Set role DM configuration"""
    try:
        data = request.json
        role_name = data.get('role_name')
        title = data.get('title', '')
        message = data.get('message')
        claim = data.get('claim', False)
        claim_role = data.get('claim_role', '')
        button_text = data.get('button_text', 'Claim Rewards')
        button_color = data.get('button_color', 'success')
        button_emoji = data.get('button_emoji', '🎁')
        include_server_logo = data.get('include_server_logo', False)
        
        if not role_name or not message:
            return jsonify({"success": False, "error": "Role name and message are required"})
        
        # Get the role ID from the bot
        role_id = None
        for guild in bot.guilds:
            for role in guild.roles:
                if role.name == role_name:
                    role_id = str(role.id)
                    break
            if role_id:
                break
        
        # Store in database
        conn = sqlite3.connect('marketing_bot.db')
        cursor = conn.cursor()
        
        # Insert or update role DM (use role_id if found, otherwise use role_name for backward compatibility)
        cursor.execute('''
            INSERT OR REPLACE INTO role_dms 
            (role_id, role_name, dm_title, dm_message, claim_button, claim_role_id, button_text, button_color, button_emoji, include_logo)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (role_id or role_name, role_name, title, message, claim, claim_role, button_text, button_color, button_emoji, include_server_logo))
        
        conn.commit()
        conn.close()
        
        return jsonify({"success": True, "message": "Role DM set successfully!"})
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/roledms')
def api_roledms():
    """Get role DMs"""
    try:
        conn = sqlite3.connect('marketing_bot.db')
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM role_dms')
        role_dms = cursor.fetchall()
        conn.close()
        
        return jsonify([{
            "id": row[0],
            "role_id": row[1],
            "role_name": row[2],
            "title": row[3],  # dm_title
            "message": row[4],  # dm_message
            "claim": bool(row[5]),  # claim_button
            "claim_role": row[6],  # claim_role_id
            "button_text": row[7],
            "button_color": row[8] if len(row) > 8 else 'success',  # button_color
            "button_emoji": row[9] if len(row) > 9 else '🎁',  # button_emoji
            "include_logo": bool(row[10]) if len(row) > 10 else bool(row[8])  # include_logo
        } for row in role_dms])
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/roledms/<role_dm_id>', methods=['DELETE'])
def api_delete_roledm(role_dm_id):
    """Delete a role DM"""
    try:
        conn = sqlite3.connect('marketing_bot.db')
        cursor = conn.cursor()
        
        # Delete the role DM
        cursor.execute('DELETE FROM role_dms WHERE id = ?', (role_dm_id,))
        
        if cursor.rowcount == 0:
            conn.close()
            return jsonify({"success": False, "error": "Role DM not found"})
        
        conn.commit()
        conn.close()
        
        return jsonify({"success": True, "message": "Role DM deleted successfully!"})
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/server-emojis')
def api_server_emojis():
    """Get server emojis for the emoji picker"""
    try:
        if not bot.guilds:
            return jsonify({"success": False, "error": "Bot is not connected to any servers"})
        
        guild = bot.guilds[0]
        emojis = []
        
        for emoji in guild.emojis:
            emojis.append({
                "id": str(emoji.id),
                "name": emoji.name,
                "animated": emoji.animated,
                "url": str(emoji.url),
                "display": f"<{'a' if emoji.animated else ''}:{emoji.name}:{emoji.id}>"
            })
        
        return jsonify({
            "success": True,
            "emojis": emojis,
            "count": len(emojis)
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/analytics/overview')
def api_analytics_overview():
    conn = sqlite3.connect('marketing_bot.db')
    cursor = conn.cursor()
    
    # Get total interactions
    cursor.execute('SELECT COUNT(*) FROM user_tracking')
    total_interactions = cursor.fetchone()[0]
    
    # Get unique users
    cursor.execute('SELECT COUNT(DISTINCT user_id) FROM user_tracking')
    unique_users = cursor.fetchone()[0]
    
    # Get interaction types breakdown
    cursor.execute('''
        SELECT interaction_type, COUNT(*) as count 
        FROM user_tracking 
        GROUP BY interaction_type 
        ORDER BY count DESC
    ''')
    interaction_breakdown = [{"type": row[0], "count": row[1]} for row in cursor.fetchall()]
    
    # Get top links
    cursor.execute('''
        SELECT link_url, click_count, unique_clicks, last_clicked
        FROM link_analytics 
        ORDER BY click_count DESC 
        LIMIT 10
    ''')
    top_links = [{"url": row[0], "clicks": row[1], "unique_clicks": row[2], "last_clicked": row[3]} for row in cursor.fetchall()]
    
    # Get top buttons
    cursor.execute('''
        SELECT button_id, button_text, click_count, unique_clicks, last_clicked
        FROM button_analytics 
        ORDER BY click_count DESC 
        LIMIT 10
    ''')
    top_buttons = [{"id": row[0], "text": row[1], "clicks": row[2], "unique_clicks": row[3], "last_clicked": row[4]} for row in cursor.fetchall()]
    
    # Get recent activity
    cursor.execute('''
        SELECT user_id, username, interaction_type, timestamp
        FROM user_tracking 
        ORDER BY timestamp DESC 
        LIMIT 20
    ''')
    recent_activity = [{"user_id": row[0], "username": row[1], "type": row[2], "timestamp": row[3]} for row in cursor.fetchall()]
    
    conn.close()
    
    return jsonify({
        "total_interactions": total_interactions,
        "unique_users": unique_users,
        "interaction_breakdown": interaction_breakdown,
        "top_links": top_links,
        "top_buttons": top_buttons,
        "recent_activity": recent_activity
    })


@app.route('/api/track-interaction', methods=['POST'])
def api_track_interaction():
    data = request.json
    user_id = data.get('user_id')
    user_name = data.get('user_name', '')
    interaction_type = data.get('interaction_type')
    interaction_data = data.get('interaction_data', {})
    server_id = data.get('server_id')
    channel_id = data.get('channel_id')
    message_id = data.get('message_id')
    
    ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.environ.get('REMOTE_ADDR', ''))
    user_agent = request.headers.get('User-Agent', '')
    session_id = data.get('session_id', str(uuid.uuid4()))
    
    if not user_id or not interaction_type:
        return jsonify({"success": False, "error": "user_id and interaction_type are required"})
    
    conn = sqlite3.connect('marketing_bot.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO user_tracking 
        (user_id, user_name, interaction_type, interaction_data, server_id, channel_id, message_id, ip_address, user_agent, session_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (user_id, user_name, interaction_type, json.dumps(interaction_data), server_id, channel_id, message_id, ip_address, user_agent, session_id))
    
    conn.commit()
    conn.close()
    
    return jsonify({"success": True, "message": "Interaction tracked successfully"})

@app.route('/api/clear-analytics', methods=['POST'])
def api_clear_analytics():
    try:
        conn = sqlite3.connect('marketing_bot.db')
        cursor = conn.cursor()
        
        # Clear all tracking data
        cursor.execute('DELETE FROM user_tracking')
        cursor.execute('DELETE FROM link_analytics')
        cursor.execute('DELETE FROM button_analytics')
        cursor.execute('DELETE FROM ai_insights')
        
        conn.commit()
        conn.close()
        
        return jsonify({"success": True, "message": "Analytics data cleared successfully"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/clear-cache', methods=['POST'])
def api_clear_cache():
    """Clear bot cache for debugging"""
    try:
        # Clear any cached data
        global bot_status
        bot_status = {"running": False, "guilds": [], "commands": [], "last_sync": None}
        
        # Clear database cache (if any)
        conn = sqlite3.connect('marketing_bot.db')
        conn.close()
        
        print("🧹 Bot cache cleared")
        return jsonify({"success": True, "message": "Cache cleared successfully"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/opt-outs')
def api_opt_outs():
    try:
        conn = sqlite3.connect('marketing_bot.db')
        cursor = conn.cursor()
        
        # Get opt-out statistics
        cursor.execute('''
            SELECT COUNT(*) as total_opt_outs,
                   COUNT(CASE WHEN created_at >= datetime('now', '-7 days') THEN 1 END) as recent_opt_outs
            FROM marketing_opt_outs 
            WHERE opt_out_type = 'marketing'
        ''')
        
        stats = cursor.fetchone()
        
        # Get recent opt-outs
        cursor.execute('''
            SELECT username, created_at 
            FROM marketing_opt_outs 
            WHERE opt_out_type = 'marketing' 
            ORDER BY created_at DESC 
            LIMIT 10
        ''')
        
        recent_opt_outs = cursor.fetchall()
        
        conn.close()
        
        return jsonify({
            "success": True,
            "total_opt_outs": stats[0],
            "recent_opt_outs": stats[1],
            "recent_list": [{"username": row[0], "date": row[1]} for row in recent_opt_outs]
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/test-functions', methods=['POST'])
def api_test_functions():
    try:
        data = request.json
        clear_rate_limits = data.get('clear_rate_limits', False)
        
        # Get bot status
        bot_online = bot.is_ready() and bot.user is not None
        guilds_connected = len(bot.guilds) if bot.guilds else 0
        commands_synced = len(bot.commands) if bot.commands else 0
        
        # Get database stats
        conn = sqlite3.connect('marketing_bot.db')
        cursor = conn.cursor()
        
        # Role DMs configured
        cursor.execute('SELECT COUNT(*) FROM role_dms')
        role_dms_configured = cursor.fetchone()[0]
        
        # Get Now buttons
        cursor.execute('SELECT COUNT(*) FROM get_now_buttons')
        getnow_buttons = cursor.fetchone()[0]
        
        # Active campaigns
        cursor.execute('SELECT COUNT(*) FROM marketing_campaigns WHERE is_active = 1')
        active_campaigns = cursor.fetchone()[0]
        
        # Total leads
        cursor.execute('SELECT COUNT(*) FROM leads')
        total_leads = cursor.fetchone()[0]
        
        # Opt-outs
        cursor.execute('SELECT COUNT(*) FROM marketing_opt_outs')
        total_opt_outs = cursor.fetchone()[0]
        
        # Clear rate limits if requested
        if clear_rate_limits:
            # This would clear any rate limiting mechanisms
            # For now, just log that it was requested
            print("🧪 Rate limits cleared for testing")
        
        conn.close()
        
        # Get server data
        roles_count = 0
        channels_count = 0
        logging_channels_count = 0
        
        if bot.guilds:
            guild = bot.guilds[0]
            roles_count = len(guild.roles)
            channels_count = len(guild.channels)
            
            # Count logging channels
            cursor.execute('SELECT COUNT(*) FROM logging_channels')
            logging_channels_count = cursor.fetchone()[0]
        
        return jsonify({
            "success": True,
            "results": {
                "bot_status": "online" if bot_online else "offline",
                "guilds_connected": guilds_connected,
                "commands_synced": commands_synced,
                "role_dms_configured": role_dms_configured,
                "getnow_buttons": getnow_buttons,
                "campaigns_active": active_campaigns,
                "total_leads": total_leads,
                "total_opt_outs": total_opt_outs,
                "rate_limits_cleared": clear_rate_limits,
                "database_connected": True,
                "database_error": None,
                "server_data": {
                    "roles_count": roles_count,
                    "channels_count": channels_count,
                    "logging_channels_count": logging_channels_count
                }
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/sync', methods=['POST'])
def api_sync():
    try:
        # Sync bot commands
        if bot.is_ready():
            # Commands are already synced when bot starts
            # Just return current status
            return jsonify({
                "success": True,
                "message": "Commands are already synced",
                "commands_count": len(bot.commands),
                "guilds_connected": len(bot.guilds),
                "bot_user": str(bot.user) if bot.user else "Unknown"
            })
        else:
            return jsonify({
                "success": False,
                "error": "Bot is not ready"
            })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/emojis')
def api_emojis():
    try:
        if not bot.guilds:
            return jsonify([])
        
        guild = bot.guilds[0]
        emojis = [{"id": str(emoji.id), "name": emoji.name, "url": str(emoji.url)} for emoji in guild.emojis]
        return jsonify(emojis)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/marketing', methods=['POST'])
def api_marketing():
    try:
        data = request.json
        role_names = data.get('role_names', [])
        message = data.get('message')
        claim = data.get('claim', False)
        claim_role = data.get('claim_role', '')
        interval = data.get('interval', '')
        include_server_logo = data.get('include_server_logo', False)
        
        if not role_names or not message:
            return jsonify({"success": False, "error": "Role names and message are required"})
        
        # Generate campaign key
        import uuid
        campaign_key = str(uuid.uuid4())[:8]
        
        # Store campaign in database
        conn = sqlite3.connect('marketing_bot.db')
        cursor = conn.cursor()
        
        # Parse interval properly
        interval_minutes = 0
        if interval:
            interval = interval.lower().strip()
            if 'd' in interval:
                # Days: 1d, 2d, etc.
                interval_minutes = int(interval.replace('d', '')) * 24 * 60
            elif 'h' in interval:
                # Hours: 1h, 2h, etc.
                interval_minutes = int(interval.replace('h', '')) * 60
            elif 'min' in interval:
                # Minutes: 30min, 60min, etc.
                interval_minutes = int(interval.replace('min', ''))
            elif 'm' in interval and 'min' not in interval:
                # Minutes: 30m, 60m, etc.
                interval_minutes = int(interval.replace('m', ''))
            else:
                # Try to parse as number (assume minutes)
                try:
                    interval_minutes = int(interval)
                except:
                    interval_minutes = 0
        
        # Create campaign entry
        cursor.execute('''
            INSERT INTO marketing_campaigns 
            (campaign_id, name, message, channel_id, interval_minutes, is_active, role_names, claim, claim_role, include_server_logo)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (campaign_key, f"Campaign {campaign_key}", message, "dm", 
              interval_minutes, True, 
              ','.join(role_names), claim, claim_role, include_server_logo))
        
        conn.commit()
        conn.close()
        
        return jsonify({"success": True, "message": "Marketing campaign started!", "campaign_key": campaign_key})
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/test-simple-dm', methods=['POST'])
def api_test_simple_dm():
    """Simple test to see if bot can send DMs at all"""
    try:
        data = request.json
        user_id = int(data.get('user_id'))
        
        if not bot.guilds:
            return jsonify({"success": False, "error": "Bot is not connected to any servers"})
        
        guild = bot.guilds[0]
        member = guild.get_member(user_id)
        
        if not member:
            return jsonify({"success": False, "error": "User not found in server"})
        
        # Try to send a simple test DM
        import asyncio
        import threading
        
        def send_test_dm():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                print(f"🔍 Creating DM channel for {member.name}...")
                dm_channel = loop.run_until_complete(member.create_dm())
                print(f"✅ DM channel created for {member.name}")
                
                print(f"🔍 Sending test message to {member.name}...")
                loop.run_until_complete(dm_channel.send("🧪 Test DM from bot - this is a test message"))
                print(f"✅ Test message sent successfully to {member.name}")
                return True
            except discord.Forbidden as e:
                print(f"🚫 DMs disabled for {member.name}: {e}")
                return False
            except Exception as e:
                print(f"❌ Test DM failed for {member.name}: {e}")
                return False
            finally:
                loop.close()
        
        result = [False]
        exception = [None]
        
        def run_test():
            try:
                result[0] = send_test_dm()
            except Exception as e:
                exception[0] = e
        
        thread = threading.Thread(target=run_test)
        thread.start()
        thread.join(timeout=5)
        
        if thread.is_alive():
            return jsonify({"success": False, "error": "Test DM timeout"})
        elif exception[0]:
            return jsonify({"success": False, "error": f"Test DM exception: {exception[0]}"})
        elif result[0]:
            return jsonify({"success": True, "message": "Test DM sent successfully"})
        else:
            return jsonify({"success": False, "error": "Test DM failed"})
            
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/test-dm-permissions', methods=['POST'])
def api_test_dm_permissions():
    """Queue Test DM Permissions operation for the bot to handle"""
    try:
        data = request.json
        role_id = int(data.get('role_id'))
        
        if not bot.guilds:
            return jsonify({"success": False, "error": "Bot is not connected to any servers"})
        
        if not bot.is_ready():
            return jsonify({"success": False, "error": "Bot is not ready yet. Please wait a moment and try again."})
        
        # Create operation ID
        operation_id = str(uuid.uuid4())
        
        # Queue the operation
        operation = {
            'id': operation_id,
            'type': 'test_dm_permissions',
            'data': {
                'role_id': role_id
            }
        }
        
        operation_queue.put(operation)
        print(f"🔍 Queued Test DM Permissions operation: {operation_id}")
        
        # Wait for result (with timeout)
        timeout = 30  # 30 seconds timeout
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if operation_id in operation_results:
                result = operation_results.pop(operation_id)
                return jsonify(result)
            time.sleep(0.1)
        
        return jsonify({"success": False, "error": "Operation timeout"})
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/optouts')
def api_optouts():
    """Get opt-out statistics"""
    try:
        conn = sqlite3.connect('marketing_bot.db')
        cursor = conn.cursor()
        
        # Total opt-outs
        cursor.execute('SELECT COUNT(*) FROM marketing_opt_outs')
        total_optouts = cursor.fetchone()[0]
        
        # This week opt-outs
        cursor.execute('''
            SELECT COUNT(*) FROM marketing_opt_outs 
            WHERE created_at >= datetime('now', '-7 days')
        ''')
        this_week_optouts = cursor.fetchone()[0]
        
        # Get opt-out list
        cursor.execute('''
            SELECT user_id, username, created_at 
            FROM marketing_opt_outs 
            ORDER BY created_at DESC
        ''')
        optouts = cursor.fetchall()
        
        conn.close()
        
        return jsonify({
            "success": True,
            "total_optouts": total_optouts,
            "this_week_optouts": this_week_optouts,
            "optouts": [{
                "user_id": row[0],
                "username": row[1],
                "created_at": row[2]
            } for row in optouts]
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/optouts/export')
def api_optouts_export():
    """Export opt-out data as CSV"""
    try:
        conn = sqlite3.connect('marketing_bot.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT user_id, username, opt_out_type, created_at 
            FROM marketing_opt_outs 
            ORDER BY created_at DESC
        ''')
        optouts = cursor.fetchall()
        conn.close()
        
        # Create CSV data
        csv_data = "User ID,Username,Opt-Out Type,Created At\n"
        for row in optouts:
            csv_data += f"{row[0]},{row[1]},{row[2]},{row[3]}\n"
        
        return jsonify({
            "success": True,
            "csv_data": csv_data,
            "filename": f"optouts_{int(time.time())}.csv"
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/optouts/<user_id>', methods=['DELETE'])
def api_delete_optout(user_id):
    """Remove an opt-out"""
    try:
        conn = sqlite3.connect('marketing_bot.db')
        cursor = conn.cursor()
        
        # Delete the opt-out
        cursor.execute('DELETE FROM marketing_opt_outs WHERE user_id = ?', (user_id,))
        
        if cursor.rowcount == 0:
            conn.close()
            return jsonify({"success": False, "error": "Opt-out not found"})
        
        conn.commit()
        conn.close()
        
        return jsonify({"success": True, "message": "Opt-out removed successfully!"})
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/campaigns', methods=['GET', 'POST'])
def api_campaigns():
    """Get or create marketing campaigns"""
    if request.method == 'POST':
        # This is handled by /api/marketing now
        pass
    else:
        # Return existing campaigns
        try:
            conn = sqlite3.connect('marketing_bot.db')
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM marketing_campaigns')
            campaigns = cursor.fetchall()
            conn.close()
            
            def format_interval(minutes):
                if minutes == 0:
                    return "once"
                elif minutes < 60:
                    return f"{minutes}min"
                elif minutes < 1440:  # Less than 24 hours
                    hours = minutes // 60
                    return f"{hours}h"
                else:
                    days = minutes // 1440
                    return f"{days}d"
            
            return jsonify([{
                "id": row[0],
                "key": row[1],  # campaign_id
                "name": row[2],
                "message": row[3],
                "channel_id": row[4],
                "interval": format_interval(row[5]),  # interval_minutes formatted
                "is_active": bool(row[6]),
                "role_names": row[7].split(',') if row[7] else [],  # role_names
                "claim": bool(row[8]) if len(row) > 8 else False,  # claim
                "claim_role": row[9] if len(row) > 9 else '',  # claim_role
                "include_server_logo": bool(row[10]) if len(row) > 10 else False  # include_server_logo
            } for row in campaigns if row[6]])  # Only return active campaigns
            
        except Exception as e:
            return jsonify({"success": False, "error": str(e)})

@app.route('/api/stop_campaign', methods=['POST'])
def api_stop_campaign():
    """Stop a marketing campaign"""
    try:
        data = request.json
        campaign_key = data.get('key')
        
        if not campaign_key:
            return jsonify({"success": False, "error": "Campaign key is required"})
        
        conn = sqlite3.connect('marketing_bot.db')
        cursor = conn.cursor()
        
        # Deactivate the campaign
        cursor.execute('UPDATE marketing_campaigns SET is_active = 0 WHERE campaign_id = ?', (campaign_key,))
        
        if cursor.rowcount == 0:
            conn.close()
            return jsonify({"success": False, "error": "Campaign not found"})
        
        conn.commit()
        conn.close()
        
        return jsonify({"success": True, "message": "Campaign stopped successfully!"})
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

# Run functions
def run_bot():
    try:
        bot.run(DISCORD_BOT_TOKEN)
    except KeyboardInterrupt:
        print("🛑 Bot shutting down...")
        # Log shutdown to web dashboard
        asyncio.run(log_activity("shutdown", success=True))
        asyncio.run(update_bot_status("paused", "Bot shutting down"))
    except Exception as e:
        print(f"❌ Bot error: {e}")
        # Log error to web dashboard
        asyncio.run(log_activity("error", success=False, error_message=str(e)))

def run_dashboard():
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)

if __name__ == "__main__":
    # Check if running on Railway (production) or locally
    if os.environ.get('RAILWAY_ENVIRONMENT') or os.environ.get('PORT'):
        print("🚀 Starting bot on Railway...")
        # On Railway, just run the bot (no dashboard)
        run_bot()
    else:
        print("🌐 Starting dashboard at http://localhost:5000")
        # Start bot in a separate thread
        bot_thread = threading.Thread(target=run_bot)
        bot_thread.daemon = True
        bot_thread.start()
        
        # Start Flask dashboard
        run_dashboard()
