# medicalDataEDA
The repo is regarding [this kaggle data-set](https://www.kaggle.com/flaredown/flaredown-autoimmune-symptom-tracker)
So It has the following columns :
'user_id', 'age', 'sex', 'country', 'checkin_date', 'trackable_id', 'trackable_type', 'trackable_name', 'trackable_value'

I've used pyspark in this project instead of scala-spark for a change.

If you see the code I've used the following dictionary structure :

{ 'Condition': { 'newTrackerId': list( of different symptoms ) } }

If you go throught the data-set's description, they've mentioned if its possible to create a system like netflix to be able to compare the different conditions and recommed medication and so on.

So for that reason my initial approach to this was as abov.

The rest of the treatment data and dosage data is structured as follows:

treatment data :
{'newTrackerData' : list( of medication taklen by the patient of this trackerId )}

dosage data:
{'newTrackerData' : { 'medicationName':'dosage of the medication' }}
