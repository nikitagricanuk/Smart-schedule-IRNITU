import os
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
COPIES = [
    os.path.join(HERE, '..', 'tools', 'dated_lessons.py'),
    os.path.join(HERE, '..', '..', 'functions_api', 'functions', 'dated_lessons.py'),
]


class TestDatedLessonsCopies(unittest.TestCase):
    def test_both_copies_are_identical(self):
        # Сервисы собираются в отдельные docker-образы, поэтому модуль скопирован; правьте обе копии.
        contents = []
        for path in COPIES:
            with open(path, encoding='utf-8') as file:
                contents.append(file.read())
        self.assertEqual(contents[0], contents[1])


if __name__ == '__main__':
    unittest.main()
