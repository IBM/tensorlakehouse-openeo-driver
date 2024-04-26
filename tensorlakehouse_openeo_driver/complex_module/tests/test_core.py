"""Unit test for complex_module.core."""

__copyright__ = """
LICENSED INTERNAL CODE. PROPERTY OF IBM.
IBM Research Licensed Internal Code
(C) Copyright IBM Corp. 2023
ALL RIGHTS RESERVED
"""

import unittest
from tensorlakehouse_openeo_driver.complex_module.core import salutation


class CoreTestCase(unittest.TestCase):
    """CoreTestCase class."""

    def setUp(self):
        """Setting up the test."""
        pass

    def test_salutation(self):
        """Test salutation()."""
        self.assertEqual(salutation(), "Gruezi Mitenand")

    def tearDown(self):
        """Tear down the tests."""
        pass
