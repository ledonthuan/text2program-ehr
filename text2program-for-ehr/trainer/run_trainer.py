import os
import logging
import sys
sys.path.append(os.getcwd())

from pandas.core.indexing import check_bool_indexer
import pytorch_lightning as pl
import torch

from transformers import HfArgumentParser

from utils.data_args import DataTrainingArguments
from utils.model_args import ModelArguments
from utils.training_args import TrainingArguments

from data_loader.data_loader import Text2TraceDataModule
from model.pl_model import Text2TraceForTransformerModel, Text2TraceForUnilmModel


logger = logging.getLogger(__name__)


def get_trainer_config(training_args):
    # Dynamically determine GPU availability
    if torch.cuda.is_available():
        num_gpus = torch.cuda.device_count()
        accelerator = "ddp" if num_gpus > 1 else None
    else:
        num_gpus = 0
        accelerator = None

    config = {
        "overfit_batches": 100,
        "max_epochs": training_args.num_train_epochs,
        "max_steps": None,
        "precision": 32,
        "gpus": num_gpus,
        "accelerator": accelerator,
        "log_every_n_steps": 50,
        "check_val_every_n_epoch": 1,
        "num_sanity_val_steps": 0,
    }
    logger.info(config)
    return config


def find_best_ckpt_by_metric(output_dir, target_metric='val_ex_acc', best_method='max'):
    files = os.listdir(output_dir)  # List files
    if target_metric == 'val_ex_acc':
        scores = [f.split(f'{target_metric}=')[-1].replace('.ckpt', '') for i, f in enumerate(files)]
        print(scores)
        #scores = [float(f.split(f'{target_metric}=')[-1].replace('.ckpt', '')) for i, f in enumerate(files)]
        best_method = 'max'
    elif target_metric == 'val_loss':
        scores = [float(f.split(f'-')[1].split(f'{target_metric}=')[-1]) for i, f in enumerate(files)]
        best_method = 'min'

    # Get the index of ckpt having best metric
    if best_method == 'max':
        best_idx = scores.index(max(scores))
    elif best_method == 'min':
        best_idx = scores.index(min(scores))
    else:
        raise ValueError()
    return os.path.join(output_dir, files[best_idx])


def main():
    # Parse arguments
    parser = HfArgumentParser((ModelArguments, DataTrainingArguments, TrainingArguments))
    model_args, data_args, training_args = parser.parse_args_into_dataclasses()

    # Set seed
    pl.seed_everything(training_args.seed)
    
    # Integrate with TensorBoard
    tnesorboard_config = {}
    tnesorboard_config.update(vars(model_args))
    tnesorboard_config.update(vars(training_args))
    
    logger = pl.loggers.TensorBoardLogger(name=training_args.run_name, save_dir=os.path.join(os.getcwd(),'saved'))
    logger.log_hyperparams(tnesorboard_config)
        
    # Gather the arguments
    triple_args = {"data_args": data_args, "model_args": model_args, "training_args": training_args}
    
    # Define pl.DataModule and pl.LightningModule
    if model_args.encoder_decoder_type == 'unilm':
        if model_args.model_name_or_path == 'bert-base-uncased':
            data_module = Text2TraceDataModule(**triple_args)
            model = Text2TraceForUnilmModel(**triple_args, tokenizer=data_module.tokenizer)
            
        elif model_args.model_name_or_path.startswith(os.getcwd()):
            pt_pl_model_path = triple_args['model_args'].model_name_or_path
            pt_ckpt_path = find_best_ckpt_by_metric(output_dir=pt_pl_model_path, target_metric='val_loss', best_method='min')
            triple_args['model_args'].model_name_or_path = 'bert-base-uncased'
            
            data_module = Text2TraceDataModule(**triple_args)
            model = Text2TraceForUnilmModel.load_from_checkpoint(checkpoint_path=pt_ckpt_path, **triple_args)
            
    elif model_args.encoder_decoder_type == 't5':
        custom_pretrained_model = False
        if model_args.model_name_or_path.startswith(os.getcwd()):
            custom_pretrained_model = True
            pt_pl_model_path = triple_args['model_args'].model_name_or_path
            pt_ckpt_path = find_best_ckpt_by_metric(output_dir=pt_pl_model_path, target_metric='val_loss', best_method='min')
            model_name = 't5-base'
            triple_args['model_args'].model_name_or_path = model_name
            triple_args['model_args'].encoder_name_or_path = model_name
            triple_args['model_args'].decoder_name_or_path = model_name
            data_module = Text2TraceDataModule(**triple_args)
        else:
            data_module = Text2TraceDataModule(data_args, model_args, training_args)

        if model_args.encoder_decoder_type == 't5':
            if custom_pretrained_model:
                model = Text2TraceForTransformerModel.load_from_checkpoint(checkpoint_path=pt_ckpt_path, **triple_args)
            else:
                model = Text2TraceForTransformerModel(model_args, training_args, data_args, data_module.tokenizer)
        
    # Define the Trainer
    trainer = pl.Trainer(
        **get_trainer_config(training_args),
        logger=logger,
    )


    #debuggin 
    print("Data Module:", data_module)
    #print("Train dataloader:", data_module.train_dataloader())
    print("Val dataloader:", data_module.val_dataloader())
    
    train_loader = data_module.train_dataloader()
    
    #for batch in train_loader:
        #print("First batch: ", batch)
        #break
        # for k, v in batch.items():
        #     print(f"Batch key: {k}, type: {type(v)}")
        #     break
    
    # Train & Validation
    if training_args.do_train:
        trainer.fit(model, data_module)
    
    # Test (decode)
    if training_args.do_predict:
        if not training_args.do_train:
            print("Hello")
            data_module.prepare_data()
            data_module.setup('test')

        # Load checkpoint
        target_metric = 'val_ex_acc'
        
        #ckpt_path = find_best_ckpt_by_metric(output_dir=training_args.output_dir, target_metric=target_metric)
        ckpt_path = find_best_ckpt_by_metric(output_dir='/home/azureuser/text2program-for-ehr/saved/models/pretrained_models/natural/train/t5-small/ne1_lr0.0001_s2s_42', target_metric=target_metric)
        if model_args.encoder_decoder_type == 'unilm':
            model = Text2TraceForUnilmModel.load_from_checkpoint(checkpoint_path=ckpt_path, **model.hparams)
        elif model_args.encoder_decoder_type == 't5':
            model = Text2TraceForTransformerModel.load_from_checkpoint(checkpoint_path=ckpt_path, **model.hparams)
        
        # Do evaluation for both eval/test dataset using datamodule
        print(f'Data module {data_module}')
        trainer.test(model, datamodule=data_module)

if __name__ == "__main__":
    main()