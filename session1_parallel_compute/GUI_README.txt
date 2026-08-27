SALEFLOW GUI - QUICK START
==========================

This GUI is built for your existing Predict Future Sales Session 1 project.

1. Keep the dataset files in:
   session1_parallel_compute\datasets\

2. Activate your existing .venv if you use one.

3. Start the GUI using either:
   python gui.py

   OR double-click:
   RUN_GUI.bat

The GUI does not replace your original Session 1 scripts.
It calls the same scripts and reads the files generated in results/.

Tabs included:
- Pipeline
- Files & eligibility
- Join & partition key
- Baseline vs parallel
- Correctness & output
- Partition balance
- Console

Buttons:
- Run each stage separately
- Run everything
- Refresh from results/
- Render diagrams

No additional GUI package is required because it uses Tkinter.
