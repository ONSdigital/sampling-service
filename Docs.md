# Service Notes

### Arguments
A user - Methodologist
Arguments: Note, that arguments passed must be in the right order (as proceeded below).

Gives: 
- Hive Database: The database which the unit frame table the user wishes to sample
Source:
- HIVE

Gives: 
- Hive Table Name: The table name of the unit frame
Source:
- HIVE

Gives:
- A stratification properties path: A path in HDFS that is the location for the properties file
Source:
- HDFS

Gives: 
- An output directory: A directory that the sample is stored when accomplished. The Stratification DataFrame/ output is not saved. 
Source:
- HDFS

### Selection Strata
The structure of the existing selection strata can be found under;
```
uk.gov.ons.registers.model.selectionstrata
```
In the future, the selection strata are presumed to evolve and not have a static structure. It is assumed that multiple structures of the selection strata will exist with varying types of class and range filters. Instantiating different types will, of course, require new case classes that extend some base SelectionStrata.trait and then the object passed to that execution.


### Dependencies

Fundamentally at the sampling service's core, the SML is required as it houses both the Stratification and Sampling methods. We do this by adding the library as a dependency. 
####Production
This means when building in Jenkins the SML will be gathered from Artifactory.  It is assumed that the SML at this point has been pre-deployed (as it is currently). 
####Development
As Artfifactory is not accessible from off-network devices we rely on Bintray which is configured under dependencies.gradle. However, if the Bintray solution fails then installing the SML locally remains a alternative option.
