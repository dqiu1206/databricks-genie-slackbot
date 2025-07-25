# Databricks Genie Slack Bot

A powerful Slack bot that integrates with Databricks Genie to provide conversational AI capabilities for data queries and analysis. The bot can now receive and send messages from **any channel** in Slack, making it more versatile and user-friendly.

## 🚀 Enhanced Features

### 📨 **Universal Channel Support**
- **Any Channel**: The bot can now receive and send messages from any public or private channel
- **Direct Messages**: Continue to support direct messages for private conversations
- **App Mentions**: Support for @botname mentions in channels
- **Automatic Channel Management**: Bot automatically joins channels when invited and handles permissions

### 🔄 **Smart Message Handling**
- **Thread-Based Conversations**: Each Slack thread maintains its own conversation context
- **Message Queuing**: Multiple messages are processed sequentially to maintain conversation context
- **Loop Prevention**: Advanced bot message detection to prevent infinite loops
- **Duplicate Prevention**: Prevents processing the same message multiple times

### 📊 **Enhanced Query Processing**
- **SQL Query Generation**: Genie generates and executes SQL queries automatically
- **CSV File Export**: Query results are provided as downloadable CSV files
- **Large File Handling**: Smart handling of files too large for Slack upload
- **System Table Support**: Special handling for system table queries with proper guidance

### 🛡️ **Robust Error Handling**
- **Graceful Degradation**: Continues working even when some features fail
- **User-Friendly Errors**: Provides helpful error messages and suggestions
- **Automatic Retry**: Implements fallback strategies for failed operations
- **Performance Monitoring**: Real-time monitoring of bot performance and resource usage

## 📋 Prerequisites

### Slack App Configuration
1. **Create a Slack App** at [api.slack.com/apps](https://api.slack.com/apps)
2. **Enable Socket Mode** for real-time communication
3. **Configure Bot Token Scopes**:
   - `channels:history` - Read channel messages
   - `channels:read` - View basic channel info
   - `chat:write` - Send messages
   - `files:write` - Upload files
   - `groups:history` - Read private channel messages
   - `groups:read` - View private channel info
   - `im:history` - Read direct messages
   - `im:read` - View direct message info
   - `mpim:history` - Read group direct messages
   - `mpim:read` - View group direct message info
   - `users:read` - View user info

4. **Configure Event Subscriptions**:
   - `message.channels` - Channel messages
   - `message.groups` - Private channel messages
   - `message.im` - Direct messages
   - `message.mpim` - Group direct messages
   - `app_mention` - App mentions
   - `channel_joined` - Bot joins channels
   - `group_joined` - Bot joins private groups

### Databricks Configuration
1. **Databricks Workspace**: Access to a Databricks workspace
2. **Genie Space**: A configured Genie space for AI conversations
3. **Authentication**: Either OAuth2 service principal or Personal Access Token

## 🛠️ Installation

### 1. Clone the Repository
```bash
git clone <repository-url>
cd databricks-genie-slackbot
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Environment Configuration
Copy the example environment file and configure your settings:

```bash
cp env.example .env
```

Edit `.env` with your configuration:

```env
# Databricks Configuration
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_ACCESS_TOKEN=your-personal-access-token
# OR use OAuth2:
# DATABRICKS_CLIENT_ID=your-client-id
# DATABRICKS_CLIENT_SECRET=your-client-secret
GENIE_SPACE_ID=your-genie-space-id

# Slack Configuration
SLACK_APP_TOKEN=xapp-your-app-token
SLACK_BOT_TOKEN=xoxb-your-bot-token

# Optional Configuration
SHOW_SQL_QUERY=true  # Set to false to hide SQL queries in responses
```

### 4. Run the Bot
```bash
python slackbot.py
```

## 🎯 Usage

### Basic Interaction
The bot can now be used in **any channel** where it's a member:

1. **Send a message** in any channel where the bot is present
2. **Mention the bot** with @botname in any channel
3. **Send direct messages** for private conversations

### Conversation Features
- **Follow-up Questions**: Reply in the same thread to continue conversations
- **Context Retention**: Genie remembers previous messages in the thread
- **Thread-Based Conversations**: Each Slack thread maintains its own conversation
- **Automatic Expiration**: Conversations expire after 1 hour of inactivity

### Special Commands
- `/reset` - Clear conversation history and start fresh
- `/help` - Show available commands and features
- `/status` - Check current conversation status

### Query Results
- **CSV Files**: Query results are automatically provided as downloadable CSV files
- **Large Files**: Files too large for Slack are handled gracefully with size information
- **System Tables**: Special guidance for system table queries that may require permissions

## 🔧 Advanced Configuration

### Performance Settings
The bot includes configurable performance settings in `slackbot/config.py`:

```python
# Core limits
MAX_PROCESSED_MESSAGES = 1000
MAX_CONVERSATION_AGE = 3600  # 1 hour
SLACK_FILE_SIZE_LIMIT = 50 * 1024 * 1024  # 50MB

# Performance settings
POLL_INTERVAL = 3
MAX_WAIT_TIME = 120
```

### Channel Management
The bot automatically:
- **Joins channels** when invited
- **Checks permissions** before posting
- **Handles private channels** with proper authentication
- **Manages bot membership** across different channel types

## 🚨 Troubleshooting

### Common Issues

1. **Bot not responding in channels**
   - Ensure the bot is a member of the channel
   - Check that the bot has the required scopes
   - Verify the bot token is correct

2. **Permission errors**
   - Check Databricks workspace permissions
   - Verify Genie space access
   - Ensure proper authentication credentials

3. **File upload failures**
   - Check file size limits (50MB max)
   - Verify bot has `files:write` scope
   - Ensure channel allows file uploads

4. **Conversation context lost**
   - Conversations expire after 1 hour of inactivity
   - Use `/reset` to start fresh conversations
   - Reply in the same thread to maintain context

### Logs and Monitoring
The bot provides comprehensive logging:
- **Performance metrics** are logged every minute
- **Error details** are logged with context for debugging

## 🔒 Security Considerations

- **Token Security**: Keep Slack and Databricks tokens secure
- **Channel Access**: Bot only accesses channels where it's explicitly added
- **Data Privacy**: Query results are only shared with the requesting user
- **Conversation Isolation**: Each thread maintains separate conversation context

## 📈 Performance Features

- **Connection Pooling**: Efficient Databricks API connection management
- **Thread Pool**: Configurable worker threads for concurrent message processing
- **Queue Management**: Sequential processing within channels to maintain context

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
1. Check the troubleshooting section
2. Review the logs for error details
3. Open an issue in the repository
4. Contact your workspace administrator for Databricks-specific issues
