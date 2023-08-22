import sys
import os

# This will let us see our python_skeleton module which is located in the
# '..' directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest

import python_skeleton

class TestPythonSkeleton(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testReturnString(self):
        my_string = "Test String"
        self.assertEqual(python_skeleton.return_string(my_string), my_string)
