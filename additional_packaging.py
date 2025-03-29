import pathlib
import shutil


def additional_packaging(ta_name):
    output_path = pathlib.Path("output")

    # Copy README
    shutil.copy(pathlib.Path("README.md"), output_path)

    # Copy license
    licenses_path = output_path / ta_name / "LICENSES"
    licenses_path.mkdir(parents=True, exist_ok=True)
    shutil.copy(pathlib.Path("LICENSE.txt"), licenses_path)
