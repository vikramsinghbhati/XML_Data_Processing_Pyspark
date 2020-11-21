This repository contain reading XML file using pyspark . 
Program will perform the following step .  
1) it will read the xml file using element tree lib of Python to select child element from xml . 
2) it will first read entire xml file as one RDD ( read RDD as wholeText) .
3) then it will use element tree lib to extract child element and perform certain validation for like date format and integer format validation using xml_parse function
4) create user defined schema using set_schema() function 
5) finally it will flat the one RDD into child element RDD using flatmap and convert into Dataframe . 