select emp.company_id, emp.emp_id, reviewer.department_id as rev_dept_id,
reviewer.emp_id as rev_emp_id, r.feedback
from company.department.team.employee emp, company.department.team.employee
reviewer, company.reviews r
where emp.company_id = r.company_id and
      emp.company_id = reviewer.company_id and
      emp.emp_id = r.emp_id and
      reviewer.emp_id = r.feedback.reviewer_emp_id
order by emp.company_id, emp.emp_id
