select r1.company_id, r1.review_id, r1.emp_id, r2.feedback.comments
from company.reviews r1, company.reviews r2
where r1.company_id = r2.company_id and
      r1.feedback.reviewer_emp_id = r2.emp_id
order by r1.company_id, r1.review_id