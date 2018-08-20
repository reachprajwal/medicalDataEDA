from collections import defaultdict

#reading the csv file using pyspark
df = spark.read.csv("/file/path/to/file.csv")

initialDict = defaultdict(defaultdict)

treatmentDict = dict()

treatmentDetailsDict = dict()

#extraction of the daat in the required format mentioned below
def xtra(df,a,initialDict,treatmentDict,treatmentDetailsDict):
    """
    df:                     This is the dataframe to work on

    initialDict:            This is the dictionary holding the mapping from condition to Symptom
                            initialDict = defaultdict(defaultdict)

    treatmentDict:          This is the dictionary holding the treatments for the condition
                            treatmentDict = dict()

    treatmentDetailsDict:   This is the dictionaryholding the details of the patients medication
                            treatmentDetailsDict = dict()
    """
    #extract all unique uer_id
    patId = df.select("user_id").distinct().collect()
    #converting the format of the list of row's of spark columns into simple elements
    patId = [x.user_id for x in patId]

    for id in patId:
        #filtering out the rows with a single user_id
        t0 = df.filter(df["user_id"] == str(id))
        #create a dataframe with only symtoms data
        t0_symptoms = t0.filter(t0["trackable_type"] == "Symptom")

        #create a dataframe with only condition data
        t0_condition = t0.filter(t0["trackable_type"] == "Condition")

        #create a dataframe with only treatments data
        t0_treatment = t0.filter(t0["trackable_type"] == "Treatment")

        #extracting all the symptoms data specifically
        symptoms_collected = t0_symptoms.select("trackable_name").distinct().collect()

        #extracting all the conditions data specifically
        conditions_collected = t0_condition.select("trackable_name").distinct().collect()

        #extracting all the treatment data specifically
        treatment_collected = t0_treatment.select("trackable_name").distinct().collect()

        #extracting all the treatment data and the dosage data specifically
        treatment_val_collected = t0_treatment.select("trackable_name","trackable_value").distinct().collect()

        #structuring the symptoms data
        symptoms = [elem.trackable_name for elem in symptoms_collected]

        #structuring the condition data
        conditions = [elem.trackable_name for elem in conditions_collected]

        #structuring the treatment data
        treatments = [elem.trackable_name for elem in treatment_collected]

        # data is added in the format of {'condition' : {'newTrackerId':[list of symptoms]}}
        for elem in conditions:
            initialDict[elem].update({x:symptoms})
        treatmentDict[x] = treatments

        temp={}

        #creating a dict with all the treatment detials in the form {medication:doagae}
        for elem in treatment_val_collected:
            temp[elem.trackable_name]=elem.trackable_value
        treatmentDetailsDict[x] = temp
        temp.clear()

        #incrementing the newTrackerId
        a+=1
    return initialDict,treatmentDict,treatmentDetailsDict

initial,treatment1,treatment2 = xtra(test,a,initialDict,treatmentDict,treatmentDetailsDict)
