"""
python -m tests.database
"""
try:
    import _thread
except ImportError:
    import thread as _thread

from bigorm.database import BigQueryDatabaseContext as DatabaseContext

from tests import UNIT_TEST_PROJECT


def _open_context():
    with DatabaseContext(project=UNIT_TEST_PROJECT):
        DatabaseContext.get_session()


def test_multithread():
    with DatabaseContext(project=UNIT_TEST_PROJECT):
        pass
    thread_id = _thread.start_new_thread(_open_context, ())


if __name__ == '__main__':
    test_multithread()
