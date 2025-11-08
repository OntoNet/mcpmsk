"""
Utility functions for Onto MCP Server.
Handles cross-platform compatibility issues.
"""
import sys


def safe_print(message: str) -> None:
    """
    Safely print message with emoji/unicode handling for Windows.
    Falls back to ASCII representation if unicode fails.
    
    **IMPORTANT:** Uses stderr to avoid interfering with MCP protocol
    which requires clean JSON output on stdout.
    
    Args:
        message: Message to print (may contain emoji/unicode)
    """
    try:
        print(message, file=sys.stderr)
    except UnicodeEncodeError:
        # Replace common emoji with ASCII equivalents
        replacements = {
            '🔐': '[AUTH]',
            '📁': '[LOAD]',
            '💾': '[SAVE]',
            '🗑️': '[DEL]',
            '⚠️': '[WARN]',
            '✅': '[OK]',
            '❌': '[ERR]',
            '🔄': '[REFRESH]',
            '🚪': '[LOGOUT]',
            '🟢': '[VALID]',
            '⏰': '[EXPIRED]',
            '📊': '[INFO]',
            '👤': '[USER]',
            '📧': '[EMAIL]',
            '🆔': '[ID]',
            '🏠': '[HOME]',
            '🔧': '[WORK]',
            '📂': '[FOLDER]',
            '🌐': '[WEB]',
            '📝': '[NOTE]',
            '🚀': '[START]',
            '🎯': '[TARGET]',
            '🛡️': '[SECURE]',
        }
        
        ascii_message = message
        for emoji, replacement in replacements.items():
            ascii_message = ascii_message.replace(emoji, replacement)
        
        print(ascii_message, file=sys.stderr)


def safe_format(template: str, *args, **kwargs) -> str:
    """
    Safely format string and return ASCII-compatible version.
    
    Args:
        template: Template string
        *args: Positional arguments for formatting
        **kwargs: Keyword arguments for formatting
        
    Returns:
        Formatted string with emoji replaced if needed
    """
    try:
        formatted = template.format(*args, **kwargs)
        # Test if it can be encoded
        formatted.encode(sys.stdout.encoding or 'utf-8')
        return formatted
    except (UnicodeEncodeError, LookupError):
        # Replace emoji with ASCII and try again
        replacements = {
            '🔐': '[AUTH]', '📁': '[LOAD]', '💾': '[SAVE]', '🗑️': '[DEL]',
            '⚠️': '[WARN]', '✅': '[OK]', '❌': '[ERR]', '🔄': '[REFRESH]',
            '🚪': '[LOGOUT]', '🟢': '[VALID]', '⏰': '[EXPIRED]', '📊': '[INFO]',
            '👤': '[USER]', '📧': '[EMAIL]', '🆔': '[ID]', '🏠': '[HOME]',
            '🔧': '[WORK]', '📂': '[FOLDER]', '🌐': '[WEB]', '📝': '[NOTE]',
            '🚀': '[START]', '🎯': '[TARGET]', '🛡️': '[SECURE]',
        }
        
        ascii_template = template
        for emoji, replacement in replacements.items():
            ascii_template = ascii_template.replace(emoji, replacement)
        
        return ascii_template.format(*args, **kwargs) 