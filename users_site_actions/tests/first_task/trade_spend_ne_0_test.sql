select spend 
from {{ ref ('trade') }}
where spend == 0 