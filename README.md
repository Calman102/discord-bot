# discord-bot

## Philosophy
The design goal of this bot is to have the bare bones of the bot working
smoothly in main.py while the functionality of the bot is set up in various Cogs
organized into extension files within the "cogs" folder. The cogs folder must be
present with .py extension files for the bot to have any useful functionality.

# Setup
In order to use this bot, a file named ".env" must be included in the bot's
directory to include the token for your bot. This file can be created in a text
editor and should look similar to the example here:

> DISCORD_TOKEN=JIKDSKLFJ-THISisRANDOMtext.LoOKsLIkeAt0k3N-ASDFgHjkL
> COMMAND_PREFIX=$
> LOG_FILE=discord_bot.log
> EMBED_COLOR=0x6699cc

The DISCORD_TOKEN is the only required variable to be set. All other environment
variables' default values are specified with their descriptions below.

DISCORD_TOKEN
* Sets the bot's token for logging into discord. Instructions for obtaining a
  token are listed below.

COMMAND_PREFIX: $
* Sets the prefix used to invoke bot commands from chat.

LOG_FILE:       discord_bot.log
* Sets the path for the bot log.

EMBED_COLOR:    0x3498db  (the same value as discord.Colour.blue())
* Sets the color the bot uses when posting embed messages.

To obtain a bot token:

1.  Go to https://discord.com/developers/applications

1.  Click on the "New Application" button and choose a name for your app.

1.  Under the "Settings" menu, click the "Bot" selection.

1.  Click "Add Bot" to cause an alert to pop up saying that this action is
    irreversible. Click "Yes, Do it!" to transform your app into a bot.

1.  Below your bot's username should be a TOKEN. You can click to reveal the
    token or you can simply click the "Copy" button to add it to your clipboard.
