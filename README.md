# Table of Contents
1. [Methodology](#Method)
1. [DAG](#DAG)
2. [Questions](#Questions)

## Methodology <a name="Method"></a>
	* Since the goal of this challenge was to join two streams/sets of data and perform simple aggregate statistics
	BeamSQL was used for all operations and joins. The CSV data was first cast with a Schema to Row Beam Datatypes, then SQL queries
	were used to perform operations.

## Directed Acyclic Graph <a name="DAG"></a>

## Questions <a name="Questions"></a>

### If this were an actual project, what steps would you take to get it ready for production?
    
    * Javadoc documentation
    * Continuous Deployment
    * Continuous Integration
    * Managed Git Repo -> GCP Cloud Repositories
    * Airflow Scheduler
    * Security - API Keys, Oauth, minimum permission scope

### What would you change or improve if you had more time?

    * Design Doc
    * More Tests
    * More Logging
    * Performance Profiling
    * Input Parameter Validation
    * Performance Metrics
    * Data Validation
    
### If this were an actual project were, the requirements made clear? What could be added or clarified?

	* There were many issues with the given requirements, besides the typos : )

### In your experience, what kind of requirements work best? What kind of template would you use to define requirements for a group of developers which you were leading?
	* JIRA Tickets
	* Agile Methodology 
	* The more specific the requirements the better