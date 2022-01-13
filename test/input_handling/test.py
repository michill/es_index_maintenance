import json, unittest, os
from main import main as run_test

"""
    Tests whether a run is aborted if abort_run is set to True
"""
class Test(unittest.TestCase):
    def setUp(self):
        self.directory = os.path.dirname(os.path.abspath(__file__))
        
    # abort_run = True
    def test1_input_handling(self):
        with open(f'{self.directory}/test1_config.json') as config_file:
            test1_config = json.load(config_file)

        with self.assertRaisesRegex(Exception, "Run aborted"):
            run_test(json.dumps(test1_config['input_config']))

    # data_source = invalid
    def test2_input_handling(self):
        with open(f'{self.directory}/test2_config.json') as config_file:
            test2_config = json.load(config_file)

        with self.assertRaisesRegex(Exception, "Invalid data_source value"):
            run_test(json.dumps(test2_config['input_config']))

    # abort_run = invalid
    def test3_input_handling(self):
        with open(f'{self.directory}/test3_config.json') as config_file:
            test3_config = json.load(config_file)

        with self.assertRaisesRegex(Exception, "Invalid abort_run value"):
            run_test(json.dumps(test3_config['input_config']))

    # delete_old = false
    def test4_input_handling(self):
        with open(f'{self.directory}/test4_config.json') as config_file:
            test4_config = json.load(config_file)

        with self.assertRaisesRegex(Exception, "Invalid delete_old value"):
            run_test(json.dumps(test4_config['input_config']))

    # forcemerge_new = true
    def test5_input_handling(self):
        with open(f'{self.directory}/test5_config.json') as config_file:
            test5_config = json.load(config_file)

        with self.assertRaisesRegex(Exception, "Invalid forcemerge_new value"):
            run_test(json.dumps(test5_config['input_config']))

    # missing input field (abort_run)
    def test6_input_handling(self):
        with open(f'{self.directory}/test6_config.json') as config_file:
            test6_config = json.load(config_file)

        with self.assertRaisesRegex(Exception, "Invalid set of input fields provided"):
            run_test(json.dumps(test6_config['input_config']))

    # Input field with typo (indx_base)
    def test7_input_handling(self):
        with open(f'{self.directory}/test7_config.json') as config_file:
            test7_config = json.load(config_file)

        with self.assertRaisesRegex(Exception, "Invalid set of input fields provided"):
            run_test(json.dumps(test7_config['input_config']))

    # new_replicas = one
    def test8_input_handling(self):
        with open(f'{self.directory}/test8_config.json') as config_file:
            test8_config = json.load(config_file)

        with self.assertRaisesRegex(Exception, "Invalid new_replicas value"):
            run_test(json.dumps(test8_config['input_config']))

    # new_replicas = -1
    def test9_input_handling(self):
        with open(f'{self.directory}/test9_config.json') as config_file:
            test9_config = json.load(config_file)

        with self.assertRaisesRegex(Exception, "Invalid new_replicas value"):
            run_test(json.dumps(test9_config['input_config']))

    # env = invalid
    def test10_input_handling(self):
        with open(f'{self.directory}/test10_config.json') as config_file:
            test10_config = json.load(config_file)

        with self.assertRaisesRegex(Exception, "Invalid env value"):
            run_test(json.dumps(test10_config['input_config']))


if __name__ == "__main__":
    unittest.main()
