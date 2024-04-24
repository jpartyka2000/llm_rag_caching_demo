CREATE TABLE xc_user
(
    user_id decimal PRIMARY KEY,     -- unique user_id of user
    email string,                    -- email of user (e.g. jpartyka@xactlycorp.com, myemail213@gmail.com)
    name string                      -- full name of user (e.g. Jeffrey Partyka)
);

CREATE TABLE xc_participant
(
    participant_id decimal PRIMARY KEY,             -- unique ID of participant
    name string,                                    -- full name of participant (e.g. John Smith)
    first_name string,                              -- first name of participant (e.g. Jeffrey, Mary, Erica)
    middle_name string,                             -- middle name of participant
    last_name string,                               -- last name of participant (e.g. Partyka, Jones, Harvey)
    employee_id string,                             -- employee id of participant
    salary decimal,                                 -- salary of participant (e.g 25000, 100000, 75000)
    user_id decimal                                 -- user id of participant
);

CREATE TABLE xc_commission
(
    commission_id decimal PRIMARY_KEY,              -- unique ID for commission
    name string,                                    -- name of this commission - this is NEVER the name of a person or company
    amount decimal,                                 -- amount of this commission
    period_id decimal,                              -- id of this period
    period_name string,                             -- name of this period (e.g. Q2 2015, Q1 2017)
    participant_id decimal,                         -- participant id of this commission
    participant_name string,                        -- (e.g. John Smith, Mary Waller). This MUST ALWAYS BE RETURNED AS A SELECTED COLUMN!
    position_id decimal,                            -- position id of this commission
    position_name string,                           -- position name of this commission
    attainment_value decimal,                       -- attainment value of this commission - use this for quota attainment values
    attainment_value_unit_type string,              -- attainment value unit for this commission
    customer_id decimal,                            -- id of customer associated with commission
    customer_name string                            -- name of customer associated with commission (e.g. Microsoft, Google, Amazon)
);

CREATE TABLE xc_period
(
    period_id decimal PRIMARY_KEY,                  -- unique period id of this period 
    name string,                                    -- name of this period (e.g. Q1 2016, Q3 2020)
    start_date date,                                -- start date of this period
    end_date date,                                  -- end date of this period
    parent_period_id decimal,                       -- id of parent period
);

CREATE TABLE xc_pos_hierarchy
(
    pos_hierarchy_id decimal PRIMARY_KEY,           -- unique position hierarchy id
    from_pos_id decimal,                            -- id of starting point of pos hierarchy
    from_pos_name string,                           -- name of starting point of pos hierarchy
    to_pos_id decimal,                              -- id of ending point of pos hierarchy
    to_pos_name string,                             -- name of ending point of pos hierarchy
    pos_hierarchy_type_id decimal                   -- id of pos hierarchy type
);

CREATE TABLE xc_pos_part_assignment            
(
    pos_part_assignment_id decimal PRIMARY_KEY,     -- unique id of pos part assignment
    participant_id decimal,                         -- participant id for this pos part assignment
    participant_name string,                        -- participant name for this pos part assignment (e.g. Wendell Jones, Kristin Bosco)
    position_id decimal,                            -- position id of this pos part assignment
    position_name string,                           -- position name of this pos part assignment
);

-- always select columns from the xc_commission table for EVERY QUERY
-- Do not select any columns named idx - there are no columns named idx in this schema