import logging
import os


def grey(text: str) -> str:
    return f"\x1b[90m{text}\x1b[0m"


def italic(text: str) -> str:
    return f"\x1b[3m{text}\x1b[0m"


class WoolLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        pathname: str = record.pathname
        cwd: str = os.getcwd()
        if pathname.startswith(cwd):
            record.ref = f"{os.path.relpath(pathname, cwd)}:{record.lineno}"
        else:
            record.ref = f".../{os.path.basename(pathname)}:{record.lineno}"
        return True


__log_format__: str = f"{grey(italic('pid:'))}%(process)-8d {grey(italic('process:'))}%(processName)-12s {grey(italic('thread:'))}%(threadName)-20s %(levelname)12s %(message)-60s {grey(italic('%(ref)s'))}"

formatter: logging.Formatter = logging.Formatter(__log_format__)

handler: logging.StreamHandler = logging.StreamHandler()
handler.setFormatter(formatter)
handler.addFilter(WoolLogFilter())
logging.getLogger().addHandler(handler)
