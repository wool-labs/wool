import logging
import os


def grey(text: str) -> str:
    """
    Apply grey ANSI color formatting to the given text.

    :param text: The text to format.
    :return: The formatted text with grey color.
    """
    return f"\x1b[90m{text}\x1b[0m"


def italic(text: str) -> str:
    """
    Apply italic ANSI formatting to the given text.

    :param text: The text to format.
    :return: The formatted text with italic style.
    """
    return f"\x1b[3m{text}\x1b[0m"


class WoolLogFilter(logging.Filter):
    """
    A logging filter that adds a reference to the source file and line number 
    for log records.

    The reference is relative to the current working directory if possible, 
    otherwise it includes the basename of the file.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Modify the log record to include a reference to the source file and 
        line number.

        :param record: The log record to modify.
        :return: True to indicate the record should be logged.
        """
        pathname: str = record.pathname
        cwd: str = os.getcwd()
        if pathname.startswith(cwd):
            record.ref = f"{os.path.relpath(pathname, cwd)}:{record.lineno}"
        else:
            record.ref = f".../{os.path.basename(pathname)}:{record.lineno}"
        return True


__log_format__: str = (
    f"{grey(italic('pid:'))}%(process)-8d "
    f"{grey(italic('process:'))}%(processName)-12s "
    f"{grey(italic('thread:'))}%(threadName)-20s "
    "%(levelname)12s %(message)-60s "
    f"{grey(italic('%(ref)s'))}"
)
"""
The default log format string for Wool, including process, thread, and 
reference information.
"""

formatter: logging.Formatter = logging.Formatter(__log_format__)
"""
The default log formatter for Wool, using the defined log format.
"""

handler: logging.StreamHandler = logging.StreamHandler()
"""
The default log handler for Wool, configured to output to the console.
"""
handler.setFormatter(formatter)
handler.addFilter(WoolLogFilter())
logging.getLogger().addHandler(handler)
