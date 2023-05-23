RightPx - Price Transperancy tool.
==============
This Repository contains the code for the backend processing of data.

Excellus BlueCross BlueShield 
-----------------------------

   Downloaded NPPES file from https://download.cms.gov/nppes/NPI_Files.html
   File size: 8.36 GB
   Total records – 7,485,411 
   Columns – 330
   No. of. columns used – 42.
   List of columns used: Click here


   •	There are 15 columns relating to the healthcare provider taxonomy code and the healthcare provider primary taxonomy switch.
   •	Healthcare provider primary taxonomy switch has 2 values – Y (yes), X (no).
   •	When 1 of the 15 columns in the healthcare provider primary taxonomy switch is marked as YES, the corresponding healthcare provider taxonomy code record is considered as the primary taxonomy code.
   •	There are cases in which any of the 15-healthcare provider primary taxonomy switch columns are marked as YES, in such cases, we take one of the healthcare provider taxonomy codes that are marked as X, in the healthcare provider primary taxonomy switch.
   •	274,474 (3.6%) out of 7,485,411 records did not have a primary healthcare provider taxonomy code. 
   •	The postal code column is loaded as a string datatype to retain the leading zeros.
   •	All column names are converted to hyphen-separated lowercase columns.
   •	The healthcare taxonomy code column is stripped to avoid leading and trailing zeros.
   •	All 7485411 NPIs had 10 digits.

NUCC - Taxonomy code table
--------------------------

   •	The taxonomy code csv file (nucc_taxonomy_221.csv) had 868 unique Taxonomy codes. Every other Unique Primary healthcare taxonomy code in the NPI table had a match in the Taxonomy Table.
   •	The code column is stripped to avoid leading and trailing zeros.
   •	We do an inner join between the NPPES table and the Taxonomy Code table on the healthcare primary taxonomy code in the NPPES table and the Code Taxonomy Code table.
