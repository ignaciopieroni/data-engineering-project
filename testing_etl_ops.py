from etl_code import extract,transform,log_progress,load_data,target_file
# Log the beggining of the ETL Process
log_progress("ETL Job Started")


# Log the beggining of the Extracion Process
log_progress("Extract phase Started")
extracted_data = extract()


# Log the completion of the Extraction Process
log_progress("Extract phase Ended")


# Log the beggining of the Transform Process
log_progress("Transform phase Started")
transformed_data = transform(extracted_data)
print(f"Transformed data: {transformed_data}\n")


# Log the completion of the Transform Process
log_progress("Transform phase Ended")


# Log the beggining of the Loading Process
log_progress("Load phase Started")
load_data(target_file,transformed_data)


# Log the completion of the Loading Process
log_progress("Load phase Ended")

# Log the completion of the ETL Process
log_progress("ETL Job Ended \n\n================================================")