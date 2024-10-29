import behave.__main__
import os

class BDDRunner:
    def __init__(self, features_dir="features", tags=None, report_dir="reports"):
        self.features_dir = features_dir
        self.tags = tags
        self.report_dir = report_dir

    def run_tests(self):
        os.makedirs(self.report_dir, exist_ok=True)

        args = f"{self.features_dir} --format json --outfile {self.report_dir}/CucumberReport.json"

        if self.tags:
            args += f" --tags {self.tags}"

        behave.__main__.main(args.split())

if __name__ == "__main__":
    runner = BDDRunner(
        features_dir="features",
        tags="@all",
        report_dir="reports"
    )
    runner.run_tests()
