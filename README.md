# Sample Dataflow Pipeline

This repo shows an implementation of a sample dataflow pipeline,
that parallelizes a sklearn preprocessing function on a BigQuery table.

## Usage

### Environment Setup

Install the python requirements:
```
pip install -r requirements.txt
```

### Running the pipeline

```
python sample_pipeline.py
``` 

### Modifying to your use case

To adapt this pipeline to your use case, here are some high-level steps:
1. **Modify parameters**: all necessary parameters are defined as constants at the top of the `sample_pipeline.py` file.
2. **Enable dataflow in your GCP project**: the dataflow API has to be enabled before you can start running jobs.
For more info, check out the [quickstart](https://cloud.google.com/dataflow/docs/quickstarts/quickstart-python).
3. **Test run**: do a test run of the current pipeline (without any significant changes to the code) to make sure all the settings are fine
4. **Modify pipeline**: once you're sure that your environment is fine, you can now create your own thing :)  

### Input and Output tables

You can see the original input and output tables in the `mle-exam` GCP project.

## Future Improvements

### Remove training from pipeline 

Instead of initializing and training the One-Hot Encoder every time: 
```
enc = preprocessing.OneHotEncoder()
enc.fit(BASE_SPECIES)
```
Ideally it's better to do the "training" somewhere else and save it, then we just pass the saved model to the pipeline for parallelization.
In the sample use case it's fine since the `OneHotEncoder` fits really quickly.
