set -e
CONDA_PATH="$HOME/miniconda/etc/profile.d/conda.sh"
OUTPUT_FILE="/root/projects/prod/dbt_test/output.txt"

echo "update start at: $(date '+%Y-%m-%d %H:%M:%S')" | tee -a "${OUTPUT_FILE}"

source "${CONDA_PATH}"
conda activate "test-dbt"
cd "$HOME/projects/prod/dbt_test/users_site_actions"
echo "start dbt preprocessing" | tee -a "${OUTPUT_FILE}"
dbt build --select users_actions_row >> "${OUTPUT_FILE}" 2>&1
echo "end dbt preprocessing" | tee -a "${OUTPUT_FILE}"

conda activate "UserActions"
cd "$HOME/projects/develop/UserActions"
echo "start python preprocess" | tee -a "${OUTPUT_FILE}"
python lib/user_actions/get_data.py >> "${OUTPUT_FILE}" 2>&1
echo "end python preprocess" | tee -a "${OUTPUT_FILE}"


conda activate "test-dbt"
cd "$HOME/projects/prod/dbt_test/users_site_actions"
echo "start dbt main transforms" | tee -a "${OUTPUT_FILE}"
dbt build --select +users_actions >> "${OUTPUT_FILE}" 2>&1
echo "end dbt main transforms" | tee -a "${OUTPUT_FILE}"

echo "update finish at: $(date '+%Y-%m-%d %H:%M:%S')" | tee -a "${OUTPUT_FILE}"