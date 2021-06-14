#!/usr/bin/env python
# coding: utf-8

# In[51]:


import requests
from xml.etree import ElementTree
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe


# In[3]:


can = requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_CAN.xml')


# In[4]:


can_tree = ElementTree.fromstring(can.content)


# In[78]:


to_get = [
    "Number of deaths",
    "Number of infant deaths",
    "Number of under-five deaths",
    "Mortality rate for 5-14 year-olds (probability of dying per 1000 children aged 5-14 years)",
    "Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)",
    "Estimates of number of homicides",
    "Crude suicide rates (per 100 000 population)",
    "Mortality rate attributed to unintentional poisoning (per 100 000 population)",
    "Number of deaths attributed to non-communicable diseases, by type of disease and sex",
    "Estimated road traffic death rate (per 100 000 population)",
    "Estimated number of road traffic deaths",
    "Mean BMI (kg/m&#xb2;) (crude estimate)",
    "Mean BMI (kg/m&#xb2;) (age-standardized estimate)",
    "Prevalence of obesity among adults, BMI &GreaterEqual; 30 (age-standardized estimate) (%)",
    "Prevalence of obesity among children and adolescents, BMI > +2 standard deviations above the median (crude estimate) (%)",
    "Prevalence of overweight among adults, BMI &GreaterEqual; 25 (age-standardized estimate) (%)",
    "Prevalence of overweight among children and adolescents, BMI > +1 standard deviations above the median (crude estimate) (%)",
    "Prevalence of underweight among adults, BMI < 18.5 (age-standardized estimate) (%)",
    "Prevalence of thinness among children and adolescents, BMI < -2 standard deviations below the median (crude estimate) (%)",
    "Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)",
    "Estimate of daily cigarette smoking prevalence (%)",
    "Estimate of daily tobacco smoking prevalence (%)",
    "Estimate of current cigarette smoking prevalence (%)",
    "Estimate of current tobacco smoking prevalence (%)",
    "Mean systolic blood pressure (crude estimate)",
    "Mean fasting blood glucose (mmol/l) (crude estimate)",
    "Mean Total Cholesterol (crude estimate)"
]


# In[84]:


df_cols = ['GHO','COUNTRY', 'SEX', 'YEAR', 'GHECAUSES', 'AGEGROUP', 'Display','Numeric', 'Low', 'High']
rows = []
country_codes = ['CAN','JPN', 'ARG', 'NZL', 'PER', 'CHL']
out_df = pd.DataFrame(rows, columns = df_cols) 
for country in country_codes:
    can = requests.get('http://tarea-4.2021-1.tallerdeintegracion.cl/gho_' + country + '.xml')
    can_tree = ElementTree.fromstring(can.content)
    new_rows = []
    for child in can_tree:
        yes = False
        for sub_child in child:
            if sub_child.tag == "GHO" and sub_child.text in to_get:
                yes = True
                s_GHO = sub_child.text
            elif sub_child.tag == "COUNTRY":
                s_COUNTRY = sub_child.text
            elif sub_child.tag == "SEX":
                s_SEX = sub_child.text
            elif sub_child.tag =="YEAR":
                s_YEAR = sub_child.text
            elif sub_child.tag =="GHECAUSES":
                s_GHECAUSES = sub_child.text
            elif sub_child.tag =="AGEGROUP":
                s_AGEGROUP = sub_child.text
            elif sub_child.tag =="Display":
                s_Display = sub_child.text
            elif sub_child.tag =="Numeric":
                s_Numeric = sub_child.text
            elif sub_child.tag =="Low":
                s_Low = sub_child.text
            elif sub_child.tag =="High":
                s_High = sub_child.text
        if yes:
            new_rows.append({'GHO': s_GHO, 'COUNTRY': s_COUNTRY, "SEX": s_SEX, "YEAR":s_YEAR, "GHECAUSES":s_GHECAUSES,
                       "AGEGROUP": s_AGEGROUP, "Display": s_Display, "Numeric": s_Numeric, "Low": s_Low, "High": s_High}) 
    temp_dataframe = pd.DataFrame(new_rows, columns = df_cols)
    out_df = pd.concat([out_df, temp_dataframe])
    print(country, "done!")


# In[85]:


out_df['Numeric'] = pd.to_numeric(out_df["Numeric"], downcast="float")
out_df['Low'] = pd.to_numeric(out_df["Low"], downcast="float")
out_df['High'] = pd.to_numeric(out_df["High"], downcast="float")


# In[86]:


gc = gspread.service_account(filename="vital-octagon-281421-8718d5647276.json")
sh = gc.open_by_key('1Kl7pPq9YjFsSj-9Q2tcGE0R6Bi5pNdHrw3cJmK6dT_0')
worksheet = sh.get_worksheet(0)

set_with_dataframe(worksheet, out_df)


# In[ ]:




