import pdb

import behave.__main__
import os
import sys

class BDDRunner:
    def __init__(self, features_dir="features", tags=None, report_dir="reports"):
        self.features_dir = features_dir
        self.tags = tags
        self.report_dir = report_dir

    def run_tests(self):
        path_features = os.path.join(os.path.dirname(__file__), self.features_dir)

        os.makedirs(self.report_dir, exist_ok=True)

        args = f"{path_features} --format json --outfile {self.report_dir}/CucumberReport.json"

        if self.tags:
            args += f" --tags {self.tags}"

        behave.__main__.main(args.split())

if __name__ == "__main__":
    it_test_path = os.path.dirname(__file__)
    src_path = os.path.abspath(os.path.join(it_test_path, "../../"))
    sys.path.insert(0, src_path)
    sys.path.insert(0, it_test_path)
    #os.environ["PYTHONPATH"] = [src_path, it_test_path]
    #sys.path.append(src_path)
    #additional_paths = [src_path, it_test_path]
    #os.environ['PYTHONPATH'] = os.pathsep.join(additional_paths)
    #pdb.set_trace()  # Interrompe a execução aqui

    runner = BDDRunner(
        features_dir="features",
        tags="@all",
        report_dir="reports"
    )
    runner.run_tests()