
#Uses bash to read the file, without linux would just guess
#!/bin/bash

#Makes sure everything stops if there is a failure, so if ingesting fails, it wont go to processing
set -e

python scripts/ingest_data.py --game-name Drip --tag-line Drip2 --region americas
python scripts/process_data.py
python scripts/train_model.py