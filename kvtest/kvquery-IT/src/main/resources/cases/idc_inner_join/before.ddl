
############### Tables for Functional Testing #############

CREATE TABLE company (
  company_id INTEGER,
  name STRING,
  head_office_location MAP(STRING),
  founders ARRAY(STRING),
  PRIMARY KEY (company_id)
)

CREATE TABLE company.department (
  department_id INTEGER,
  name STRING,
  budget_breakdown MAP(DOUBLE),
  established TIMESTAMP(4),
  PRIMARY KEY (department_id)
)

CREATE TABLE company.reviews (
  review_id LONG,
  emp_id LONG,
  feedback JSON,
  PRIMARY KEY (review_id)
)

CREATE TABLE company.project (
  project_id INTEGER,
  client_id INTEGER,
  name STRING,
  project_milestones MAP(STRING),
  PRIMARY KEY (project_id)
)

CREATE TABLE company.client (
  client_id INTEGER,
  name STRING,
  preferred_contact_methods ARRAY(STRING),
  PRIMARY KEY (client_id)
)

CREATE TABLE company.skill (
  skill_id INTEGER,
  skill_value INTEGER,
  name STRING,
  PRIMARY KEY (skill_id)
)

CREATE TABLE company.department.team (
  team_id INTEGER,
  name STRING,
  technologies_used ARRAY(STRING),
  PRIMARY KEY (team_id)
)

CREATE TABLE company.department.team.employee (
  emp_id LONG,
  name STRING,
  projects ARRAY(INTEGER),
  skills ARRAY(INTEGER),
  contact_info MAP(STRING),
  PRIMARY KEY (emp_id)
)

CREATE TABLE company.no_records (
  record_id INTEGER,
  PRIMARY KEY (record_id)
)

CREATE TABLE company.null_records (
  record_id INTEGER,
  value INTEGER,
  PRIMARY KEY (record_id)
)

CREATE TABLE org (
    company_id INTEGER,
    name STRING,
    PRIMARY KEY (company_id)
)

