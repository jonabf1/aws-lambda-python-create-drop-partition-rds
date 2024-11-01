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
        absolute_path = os.path.dirname(__file__)
        path_features = os.path.join(absolute_path, self.features_dir)

        os.makedirs(os.path.join(absolute_path, self.report_dir), exist_ok=True)

        args = f"{path_features} --format json --outfile {os.path.join(absolute_path, self.report_dir)}/CucumberReport.json"

        if self.tags:
            args += f" --tags {self.tags}"

        behave.__main__.main(args.split())

if __name__ == "__main__":
    src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
    sys.path.append(src_path)

    runner = BDDRunner(
        features_dir="features",
        tags="@all",
        report_dir="reports"
    )
    runner.run_tests()