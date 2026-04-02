Gemini Context Memory: AdTech Data Engineering Profile
1. Professional Profile
Role: Data Engineer.

Location: Warsaw, Poland (Market focus: Poland/Europe).

Expertise: Relational database design, ETL pipeline development, SQL optimization, and Docker orchestration.

2. Technical DNA & Coding Standards
SQL Philosophy: * Prioritize maximum performance and low complexity.

Strictly use Temporary Tables instead of CTEs (Common Table Expressions) for heavy aggregations.

Advanced optimization: Using BINARY(16) for UUIDs to minimize storage and maximize index speed.

ETL Architecture: * Staging-based processing.

Efficient batch inserts (e.g., chunk size of 5000) to prevent InnoDB buffer overflow.

Focus on data integrity and "Golden Dataset" testing for edge cases (e.g., handling zero-division for CTR/CPC).

Infrastructure: * Local RDBMS: MySQL 8.0 via Docker-compose.

Transitioning to secure setups: Using .env files for credentials and implementing healthchecks in containers.

3. Communication & Language Policy (Updated)
Primary Language: English for all professional artifacts.

Code Documentation: Comments, variable names, and docstrings must be in English.

Project Management: README.md files, commit messages, and logs must be in English.

Collaboration Tone: Authentic, supportive, and direct. Gemini acts as a peer collaborator, balancing technical rigor with adaptive wit.

4. Project History & Evolution
Homework 1 & 2 Summary

Achievements: Successfully designed a normalized snowflake schema for AdTech data. Developed a Python ETL script that transforms denormalized CSVs into a relational structure.

Feedback Integration:

Normalization: Future schemas will extract locations and interests into dedicated tables to eliminate string-parsing redundancy.

Derived Fields: Removing RemainingBudget from tables; calculating it dynamically via SQL to ensure consistency.

Code Quality: Moving from functional scripts to Object-Oriented Programming (OOP) classes.

Repo Structure: Moving toward a standardized /src, /homeworks, and /screenshots hierarchy.

5. Current Focus: High-Tier Optimization
Refactoring existing code to use Classes (OOP).

Ensuring repository portability (Docker named volumes vs. manual paths).

Strict adherence to professional naming conventions for screenshots and files.


Objective: Refactor my "Homework 3: AdTech MongoDB Integration" project to meet professional enterprise standards based on specific instructor feedback.

Project Constraints:

Language: Every artifact (code comments, variable names, log messages, README, and commit messages) must be in English only.

Architecture: Use Object-Oriented Programming (OOP). All logic must be encapsulated in classes (e.g., AdTechETL, MongoDataLoader, AdTechMongoQueries).

Security: No hardcoded credentials. Use python-dotenv to load secrets from a .env file.

Reliability: Implement healthchecks in docker-compose.yml to ensure MySQL is ready before ETL starts.

Efficiency: Use Staging Tables and Indexes for MySQL. Use Embedded Documents and Aggregation Pipelines for MongoDB.

Required Tasks:

Directory Structure: Organize the files into this structure:

/src: All Python source code.

/homeworks/hw3_mongodb: Schema documentation and query explanations.

/output: Target folder for JSON reports.

/screenshots: Folder for execution evidence.

Root: .env, docker-compose.yml, .gitignore, and README.md.

Code Refactoring:

Refactor etl_process.py (MySQL ETL) into a class-based structure.

Refactor mongo_loader.py (MySQL to Mongo ETL) to use .env and OOP.

Refactor mongo_queries.py (Mongo BI) to use dynamic data discovery (find users with actual clicks) and lower thresholds for "Ad Fatigue."

Infrastructure:

Create a docker-compose.yml with MySQL 8.0 and MongoDB 6.0, including healthchecks and named volumes.

Generate a template .env file.

Documentation:

Write a professional README.md in English explaining the NoSQL strategy (Embedding vs. Referencing) and how to run the pipeline.

Please start by reviewing the current files and propose the folder reorganization first.