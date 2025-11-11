import streamlit as st
import pandas as pd
import tempfile
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Import your custom modules
from modules.darwinbox_client import DarwinboxClient
from modules.ai_analyzer import AIAnalyzer
from utils.file_processor import download_file, extract_text_from_file
from utils.file_saver import save_data, create_resume_folder
from utils.gsheets_client import GSheetsClient

# --- Page Configuration ---
st.set_page_config(page_title="Darwinbox AI Resume Analyzer", layout="wide", page_icon="🤖")

# --- Session State Initialization ---
def init_session_state():
    if "app_step" not in st.session_state:
        st.session_state.app_step = 0
    for key, default in [
        ("db_client", None), ("ai_analyzer", None), ("gsheets_client", None), ("job_list", []),
        ("selected_job_id", None), ("selected_job_code", None), ("selected_job_str", "N/A"),
        ("candidates", pd.DataFrame()), ("analysis_results", pd.DataFrame()),
        ("jd_text", ""), ("jd_file_details", None), ("jd_input_method", "Manual Input"),
        ("finalized_candidates", pd.DataFrame())
    ]:
        if key not in st.session_state:
            st.session_state[key] = default

# --- Helper Functions ---
def connect_to_services():
    try:
        st.session_state.db_client = DarwinboxClient()
        st.session_state.ai_analyzer = AIAnalyzer()
        st.session_state.gsheets_client = GSheetsClient()
        st.toast("Successfully connected to all services!", icon="✅")
        st.session_state.app_step = 1
    except Exception as e:
        st.error(f"Connection failed: {e}")
        st.session_state.app_step = 0

def reset_app():
    current_db_client = st.session_state.db_client
    current_ai_analyzer = st.session_state.ai_analyzer
    current_job_list = st.session_state.job_list
    current_gsheets_client = st.session_state.gsheets_client
    jd_method = st.session_state.get("jd_input_method", "Manual Input")
    
    for key in list(st.session_state.keys()):
        del st.session_state[key]
        
    init_session_state()
    st.session_state.db_client = current_db_client
    st.session_state.ai_analyzer = current_ai_analyzer
    st.session_state.job_list = current_job_list
    st.session_state.gsheets_client = current_gsheets_client
    st.session_state.jd_input_method = jd_method
    st.session_state.app_step = 1

def flatten_candidate_data(df):
    if 'application_data' not in df.columns: return df
    def get_biographical(x, field):
        try: return str(x.get('Biographical', {}).get(field))
        except: return None
    def get_work_experience(x):
        try:
            titles = [exp.get('Job Title') for exp in x.get('Work Experience', []) if exp.get('Job Title')]
            return ', '.join(titles) if titles else None
        except: return None
    def get_education(x):
        try:
            degrees = [edu.get('Education Degree') for edu in x.get('Education', []) if edu.get('Education Degree')]
            return ', '.join(degrees) if degrees else None
        except: return None
    df['experience_level'] = df['application_data'].apply(lambda x: get_biographical(x, 'Are you a Fresher or Experienced?'))
    df['total_experience'] = df['application_data'].apply(lambda x: get_biographical(x, 'Total Work Experience (in months)?'))
    df['notice_period'] = df['application_data'].apply(lambda x: get_biographical(x, 'Notice period'))
    df['highest_qualification'] = df['application_data'].apply(lambda x: get_biographical(x, 'Highest Educational Qualification'))
    df['work_experience_titles'] = df['application_data'].apply(get_work_experience)
    df['education_degrees'] = df['application_data'].apply(get_education)
    df['application_data'] = df['application_data'].astype(str)
    return df

def analyze_single_resume(candidate_data, jd_text, ai_analyzer, resume_save_path, api_key):
    resume_url = candidate_data.get('darwinbox_resume_url')
    result_dict = {
        'Candidate Name': candidate_data.get('name', 'N/A'),
        'Candidate ID': candidate_data.get('candidate_unique_id'),
        'Score (%)': 0,
        'Resume Link': 'N/A'
    }
    if not resume_url:
        result_dict['AI Remarks'] = 'Skipped: No resume URL found.'
        return result_dict
    result_dict['Resume Link'] = resume_url
    safe_name = "".join(c for c in candidate_data.get('name', 'candidate') if c.isalnum() or c in (' ', '_')).rstrip()
    file_id = candidate_data.get('candidate_unique_id') or candidate_data.get('candidate_id') or 'no_id'
    local_filename = f"{safe_name}_{file_id}.pdf"
    local_path = os.path.join(resume_save_path, local_filename)
    text_content = ""
    if download_file(resume_url, local_path):
        text_content = extract_text_from_file(local_path)
    else:
        text_content = "Error: Could not download resume."
    if "Error:" in text_content:
        ai_result = {'overall_score': 0, 'summary': text_content}
    else:
        ai_result = ai_analyzer.analyze_resume(text_content, jd_text, api_key)
    result_dict['Score (%)'] = ai_result.get('overall_score', 0)
    result_dict['AI Remarks'] = ai_result.get('summary', 'No summary generated.')
    return result_dict

def analyze_resume_batch(resume_batch, api_key, jd_text, ai_analyzer, resume_save_path):
    """Processes a small batch of resumes using the same API key."""
    results = []
    for resume_data in resume_batch:
        result = analyze_single_resume(resume_data, jd_text, ai_analyzer, resume_save_path, api_key)
        results.append(result)
    return results

# --- SIDEBAR DISPLAY FUNCTION ---
def display_sidebar():
    with st.sidebar:
        st.title("📊 Analysis Dashboard")
        st.divider()
        
        st.caption("JOB INFORMATION")
        st.metric("Total Open Jobs", len(st.session_state.job_list) if st.session_state.job_list else "N/A")
        
        if st.session_state.app_step >= 2:
            st.markdown(f"**Selected Job:** {st.session_state.get('selected_job_str', 'N/A')}")
            st.metric("Candidates to Analyze", len(st.session_state.candidates))
        
        if st.session_state.app_step >= 5:
            st.divider()
            st.caption("AI ANALYSIS SUMMARY")
            results = st.session_state.analysis_results
            if not results.empty:
                failed_count = sum(1 for r_idx, r in results.iterrows() if 'failed' in r.get('AI Remarks', '').lower() or r.get('AI Remarks', '').startswith('Error'))
                successful_count = len(results) - failed_count
                
                col1, col2 = st.columns(2)
                col1.metric("✅ Analyzed", successful_count)
                col2.metric("❌ Failed", failed_count, delta_color="inverse")
            else:
                 st.metric("Analyzed", "0")
        
        st.divider()
        st.button("🔄 Start a New Analysis", on_click=reset_app, type="primary", use_container_width=True)

# --- UI Display Functions for Each Step ---
def display_step1_job_selection():
    with st.expander("✅ Step 1: Select a Job Role", expanded=(st.session_state.app_step == 1)):
        is_disabled = st.session_state.app_step > 1
        if not st.session_state.job_list:
            with st.spinner("Fetching open jobs..."):
                st.session_state.job_list = st.session_state.db_client.get_jobs()
                if st.session_state.job_list:
                    save_data(st.session_state.job_list, "job_list", "fetched_jobs", "json")
                    st.session_state.gsheets_client.append_data_to_sheet("Job List", st.session_state.job_list)
        if st.session_state.job_list:
            job_count = len(st.session_state.job_list)
            st.info(f"Found {job_count} open jobs.")
            job_options = {f"{job['job_title']} (ID: {job['job_code']})": (job['job_id'], job['job_code']) for job in st.session_state.job_list}
            selected_job_str = st.selectbox("Choose a job:", options=job_options.keys(), disabled=is_disabled)
            if st.button(f"Fetch Candidates", type="primary", disabled=is_disabled):
                st.session_state.selected_job_str = selected_job_str
                st.session_state.selected_job_id, st.session_state.selected_job_code = job_options[selected_job_str]
                with st.spinner("Fetching candidates..."):
                    candidates_list = st.session_state.db_client.get_candidates_for_job(st.session_state.selected_job_id)
                    df = pd.DataFrame(candidates_list)
                    if not df.empty:
                        if 'unique_id' in df.columns:
                            df.rename(columns={'unique_id': 'candidate_unique_id'}, inplace=True)
                        df_flattened = flatten_candidate_data(df.copy())
                        st.session_state.candidates = df_flattened
                        save_data(candidates_list, "candidates_data", st.session_state.selected_job_code, "json")
                        st.session_state.gsheets_client.append_data_to_sheet("Candidates Data", df_flattened.to_dict('records'))
                    else:
                        st.session_state.candidates = df
                st.session_state.app_step = 2
                st.rerun()
        else:
            st.error("Could not fetch any jobs from Darwinbox.")

def display_step2_review_candidates():
    with st.expander("✅ Step 2: Review and Filter Candidates", expanded=(st.session_state.app_step == 2)):
        is_disabled = st.session_state.app_step > 2
        if 'all_candidates' not in st.session_state:
            st.session_state.all_candidates = st.session_state.candidates.copy()
        total_candidates = len(st.session_state.all_candidates)
        if total_candidates == 0:
            st.warning("No candidates found for the selected job.")
            return
        st.info(f"Found {total_candidates} total candidates for the selected job.")
        filtered_df = st.session_state.all_candidates
        unique_statuses = []
        if 'status' in filtered_df.columns:
            unique_statuses = filtered_df['status'].dropna().unique().tolist()
        if unique_statuses:
            selected_statuses = st.multiselect(
                'Filter candidates by status:', options=unique_statuses, disabled=is_disabled
            )
            if selected_statuses:
                filtered_df = filtered_df[filtered_df['status'].isin(selected_statuses)]
        st.markdown("---")
        st.markdown(f"**Displaying {len(filtered_df)} candidates**")
        st.dataframe(filtered_df)
        if st.button("Proceed to Analysis ➡️", type="primary", disabled=is_disabled):
            st.session_state.candidates = filtered_df
            st.session_state.app_step = 3
            st.rerun()

def display_step3_provide_jd():
    with st.expander("✅ Step 3: Provide Job Requirements", expanded=(st.session_state.app_step == 3)):
        is_disabled = st.session_state.app_step > 3
        
        def clear_on_change():
            if st.session_state.jd_input_method == "Manual Input":
                st.session_state.jd_file_details = None
                st.session_state.jd_text = ""
        
        input_method = st.radio("How to provide Job Description?", ("Manual Input", "Upload File"), horizontal=True, disabled=is_disabled, key="jd_input_method", on_change=clear_on_change)
        
        if input_method == "Upload File":
            if st.session_state.jd_file_details:
                st.info(f"**Using File:** `{st.session_state.jd_file_details['name']}` ({st.session_state.jd_file_details['size']:.2f} KB)")
            uploaded_file = st.file_uploader("Upload a New JD", type=['pdf', 'docx', 'txt', 'png', 'jpg', 'jpeg'], disabled=is_disabled)
            if uploaded_file is not None and (st.session_state.jd_file_details is None or st.session_state.jd_file_details["name"] != uploaded_file.name):
                with st.spinner("Extracting text..."):
                    st.session_state.jd_file_details = {"name": uploaded_file.name, "size": uploaded_file.size / 1024}
                    with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(uploaded_file.name)[1]) as tmp:
                        tmp.write(uploaded_file.getvalue())
                        st.session_state.jd_text = extract_text_from_file(tmp.name)
                    os.unlink(tmp.name)
                    st.success("Text extracted! Review below.")
        
        st.session_state.jd_text = st.text_area("Job Description Text:", value=st.session_state.jd_text, height=200, disabled=is_disabled, placeholder="Paste JD or upload a file...")
        
        if st.button(f"🚀 Start Analysis", type="primary", disabled=(is_disabled or not st.session_state.jd_text)):
            st.session_state.app_step = 4
            st.rerun()

# --- CONCURRENT ANALYSIS FUNCTION (with Sequential Batching) ---
def display_step4_unfiltered_results():
    with st.expander("✅ Step 4: Raw Analysis Results", expanded=(st.session_state.app_step >= 4 and st.session_state.app_step < 6)):
        if st.session_state.app_step == 4:
            if st.session_state.candidates.empty:
                st.warning("No candidates were selected for analysis. Please go back to Step 2.")
                return

            # --- CONFIGURATION FOR BATCH PROCESSING ---
            BATCH_SIZE_PER_KEY = 3
            
            # --- SETUP ---
            resume_folder_path = create_resume_folder(st.session_state.selected_job_code)
            all_candidates_list = st.session_state.candidates.to_dict('records')
            total_candidates = len(all_candidates_list)
            api_keys = st.session_state.ai_analyzer.api_keys_list
            num_keys = len(api_keys)
            
            # Number of parallel workers will be the number of API keys, so each key runs its batches in parallel.
            num_workers = num_keys 
            
            progress_bar = st.progress(0, text=f"Initializing analysis for {total_candidates} candidates...")
            results_placeholder = st.empty()
            analysis_results_list = []

            # --- MODIFIED: Sequential Batch Creation Logic ---
            st.info(f"Organizing {total_candidates} resumes into sequential batches of {BATCH_SIZE_PER_KEY}...")
            
            all_batches_to_run = []
            key_index = 0
            for i in range(0, total_candidates, BATCH_SIZE_PER_KEY):
                # 1. Get a chunk of resumes
                resume_chunk = all_candidates_list[i : i + BATCH_SIZE_PER_KEY]
                
                # 2. Assign the current key to this entire chunk
                assigned_key = api_keys[key_index]
                
                # 3. Add the (chunk, key) pair to our list of work
                all_batches_to_run.append((resume_chunk, assigned_key))
                
                # 4. Cycle to the next key for the *next* batch
                key_index = (key_index + 1) % num_keys

            # --- PASS 1: Initial Analysis using Batches ---
            total_processed_count = 0
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = {
                    executor.submit(
                        analyze_resume_batch, 
                        batch_data, api_key,
                        st.session_state.jd_text, st.session_state.ai_analyzer, resume_folder_path
                    ): batch_data for batch_data, api_key in all_batches_to_run
                }

                for future in as_completed(futures):
                    try:
                        batch_results = future.result() # This is a list of results from a batch
                        analysis_results_list.extend(batch_results)
                        total_processed_count += len(batch_results)
                        
                        progress_text = f"Analyzed {total_processed_count}/{total_candidates} resumes..."
                        progress_bar.progress(total_processed_count / total_candidates, text=progress_text)
                        temp_df = pd.DataFrame(analysis_results_list)
                        results_placeholder.dataframe(temp_df)
                    except Exception as e:
                        st.error(f"An error occurred during batch analysis: {e}")
            
            st.session_state.analysis_results = pd.DataFrame(analysis_results_list)
            st.success("All resume batches have been analyzed!")
            st.session_state.app_step = 5
            # NOTE: Retry logic would need to be re-integrated here if desired. For clarity, it's omitted.
            st.rerun()
            
        if st.session_state.app_step >= 5:
            df_results = st.session_state.analysis_results.sort_values(by="Score (%)", ascending=False)
            
            if not st.session_state.get('analysis_saved', False):
                save_data(df_results, "candidates_analyzed_scores", st.session_state.selected_job_code, "csv")
                st.session_state.gsheets_client.append_data_to_sheet("AI Analysis Results", df_results.to_dict('records'))
                st.session_state.analysis_saved = True

            st.info("This table shows the complete, unfiltered results of the AI analysis, sorted by score.")
            st.dataframe(df_results)
            st.markdown("---")
            col1, col2 = st.columns([1, 2])
            with col1:
                if st.button("🔄 Re-run Full Analysis"):
                    st.session_state.analysis_results = pd.DataFrame()
                    st.session_state.finalized_candidates = pd.DataFrame()
                    st.session_state.analysis_saved = False
                    st.session_state.app_step = 4
                    st.rerun()
            with col2:
                if st.button("Proceed to Filter & Finalize ➡️", type="primary"):
                    st.session_state.app_step = 6
                    st.rerun()

def display_step5_filter_and_finalize():
    with st.expander("➡️ Step 5: Filter and Finalize with Manual Review", expanded=(st.session_state.app_step == 6)):
        if st.session_state.analysis_results.empty:
            st.warning("No analysis results to display.")
            return
        df_results = st.session_state.analysis_results
        if 'Final Status' not in df_results.columns:
            df_results['Final Status'] = 'Select...'
        st.subheader("Filter and Assign Status")
        with st.container(border=True):
            col1, col2 = st.columns(2)
            with col1:
                name_search = st.text_input("Filter by Candidate Name:")
                id_search = st.text_input("Filter by Candidate ID:") 
            with col2:
                score_range = st.slider("Filter by Score (%):", 0, 100, (0, 100))
                remarks_search = st.text_input("Search in Remarks:")
        filtered_df = df_results
        if name_search: filtered_df = filtered_df[filtered_df['Candidate Name'].str.contains(name_search, case=False, na=False)]
        if id_search: filtered_df = filtered_df[filtered_df['Candidate ID'].astype(str).str.contains(id_search, case=False, na=False)]
        if remarks_search: filtered_df = filtered_df[filtered_df['AI Remarks'].str.contains(remarks_search, case=False, na=False)]
        filtered_df = filtered_df[(filtered_df['Score (%)'] >= score_range[0]) & (filtered_df['Score (%)'] <= score_range[1])]
        st.markdown("---")
        st.markdown("Assign a final status for each candidate below:")
        column_order = ["Candidate Name", "Candidate ID", "Final Status", "Score (%)", "Resume Link", "AI Remarks"]
        df_to_display = filtered_df[column_order]
        edited_df = st.data_editor(df_to_display,
            column_config={
                "Final Status": st.column_config.SelectboxColumn("Final Status", options=['Select...', 'Selected', 'Rejected'], required=True),
                "Resume Link": st.column_config.LinkColumn("Resume Link"),
                "AI Remarks": st.column_config.TextColumn("AI Remarks", width="large")
            },
            use_container_width=True, hide_index=True, key="final_review_editor", height=min(len(df_to_display) * 36 + 36, 600))
        if not edited_df.equals(df_to_display):
            update_map = edited_df.set_index('Candidate ID')['Final Status'].to_dict()
            original_df = st.session_state.analysis_results
            original_df['Final Status'] = original_df['Candidate ID'].map(update_map).fillna(original_df['Final Status'])
            st.session_state.analysis_results = original_df
            st.rerun()
        st.markdown("---")
        if st.button("✅ Finalize"):
            finalized_df = st.session_state.analysis_results[st.session_state.analysis_results['Final Status'].isin(['Selected', 'Rejected'])]
            if finalized_df.empty:
                st.warning("Please assign a status ('Selected' or 'Rejected') to at least one candidate before finalizing.")
            else:
                st.session_state.finalized_candidates = finalized_df
                st.session_state.app_step = 7
                st.rerun()

def display_step6_final_review():
    with st.expander("➡️ Step 6: Final Review & Submit to Darwinbox", expanded=(st.session_state.app_step == 7)):
        if st.session_state.finalized_candidates.empty:
            st.warning("No candidates have been finalized yet.")
            return
        st.subheader("Finalized Candidates List")
        st.info("The following decisions will be submitted to Darwinbox.")
        st.dataframe(st.session_state.finalized_candidates[['Candidate Name', 'Candidate ID', 'Final Status', 'AI Remarks']])
        st.markdown("---")
        if st.button(f"🚀 Submit {len(st.session_state.finalized_candidates)} Decisions to Darwinbox", type="primary"):
            upload_errors, success_count = [], 0
            progress_bar = st.progress(0, text="Initializing submission...")
            status_text = st.empty()
            finalized_list = list(st.session_state.finalized_candidates.iterrows())
            total_count = len(finalized_list)
            job_identifier = st.session_state.selected_job_id
            for i, (_, row) in enumerate(finalized_list):
                candidate_id, candidate_name, decision = row['Candidate ID'], row['Candidate Name'], row['Final Status']
                status_text.text(f"Processing {i+1}/{total_count}: {candidate_name} -> {decision}")
                success, message = False, ""
                if decision == 'Selected':
                    success, message = st.session_state.db_client.shortlist_candidate(candidate_id=candidate_id, job_id=job_identifier)
                elif decision == 'Rejected':
                    rejection_tag = row['AI Remarks'] if pd.notna(row['AI Remarks']) and row['AI Remarks'].strip() else "Rejected based on screening"
                    success, message = st.session_state.db_client.reject_candidate(candidate_id=candidate_id, job_id=job_identifier, reason_tag=rejection_tag)
                if success: success_count += 1
                else: upload_errors.append(f"Failed on '{candidate_name}': {message}")
                progress_bar.progress((i + 1) / total_count)
            status_text.empty()
            st.success(f"Action completed for {success_count} candidate(s).")
            if upload_errors:
                st.error("The following errors occurred:")
                st.json(upload_errors)

# --- Main Application Logic ---
init_session_state()
display_sidebar()
st.header("🤖 Darwinbox AI Resume Analyzer")

if st.session_state.app_step == 0:
    st.info("Please ensure your credentials in `.streamlit/secrets.toml` are correct.")
    if st.button("Click to Connect and Start"):
        with st.spinner("Connecting to services..."):
            connect_to_services()
        st.rerun()
else:
    display_step1_job_selection()
    if st.session_state.app_step >= 2: display_step2_review_candidates()
    if st.session_state.app_step >= 3: display_step3_provide_jd()
    if st.session_state.app_step >= 4: display_step4_unfiltered_results()
    if st.session_state.app_step >= 6: display_step5_filter_and_finalize()
    if st.session_state.app_step >= 7: display_step6_final_review()