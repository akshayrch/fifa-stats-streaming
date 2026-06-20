# Notebooks

Ad-hoc exploration notebooks (EDA on Bronze/Silver/Gold tables, prototyping
features before they're productionized into `streaming/jobs/` or `ml/`).

Convention: prefix with the phase number, e.g. `phase1_producer_smoke_test.ipynb`,
`phase3_silver_eda.ipynb`. Keep notebooks lightweight — once logic stabilizes,
move it into `streaming/jobs/` or `ml/<app>/src/`.
