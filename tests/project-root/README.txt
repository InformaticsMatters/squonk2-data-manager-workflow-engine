The 'project-root' directory where the UnitTestInstanceLauncher creates the test
project directory, simulating the file-system environment of the DataManager.
Any and all files generated by step Jobs will be written to the test project directory
(`project-00000000-0000-0000-0000-000000000001`).

The directory is created (and wiped) every time a UnitTestInstanceLauncher is created.
