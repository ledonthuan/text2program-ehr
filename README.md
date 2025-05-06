This repo is for UIUC CS 598 Deep Learning for Healthcare final project where we attempted to replicate findings from Uncertainty-Aware Text-to-Program for Question Answering on Structured Electronic Health Records (https://arxiv.org/abs/2203.06918) research paper.

The packages for the environment are saved in requirements.txt.
The main file used to recreate the steps is text2program.ipynb. All the steps are in the Jupyter notebook but the main commands to run are in the following order - there are other steps needed to move data around so that the different repo scripts are able to access the necessary data points for their scripts:


1. Download the MIMIC-III Database - make sure to unzip the CSVs as they are .gz when they come
2. !git clone https://github.com/wangpinggl/TREQS.git
3. !python process_mimic_db.py
4. !git clone https://github.com/junwoopark92/mimic-sparql.git
5. !PYTHONPATH=mimic_sparql python mimic_sparql/build_mimicsqlstar_db/build_mimicstar_db_from_mimicsql_db.py
6. !PYTHONPATH=mimic_sparql python mimic_sparql/build_mimicsparql_kg/build_complex_kg_from_mimicsqlstar_db.py
7. !git clone https://github.com/cyc1am3n/text2program-for-ehr
8. !PYTHONPATH=text2program-for-ehr/data python text2program-for-ehr/data/preprocess.py
9. !python main.py --train_batch_size 8 --eval_batch_size 8 --num_train_epochs 1 


Different Repos and their purposes:
- https://github.com/wangpinggl/TREQS.git: build select mimic.db using only 100 unique ids across 9 different MIMIC-III tables
- https://github.com/junwoopark92/mimic-sparql.git: build the knowledge graph for the model to traverse and find answers
- https://github.com/cyc1am3n/text2program-for-ehr: train the seq2seq model to translate natural language to programs


Results:
Our results are shown at the end of the text2program.ipynb file - unfortunately the scripts do not work completely out of the box leading to much time spent debugging and in the end we had and accuracy of 0.00. This is most likely due to the parsing of the value 'answers' not lining up correctly.
