function PythonSpecificSettings()
  setlocal colorcolumn=88
endfunction
autocmd FileType python call PythonSpecificSettings()

let g:python3_host_prog = '~/.python-envs/de-course-env/bin/python3'

let g:ale_fix_on_save = 1
let g:ale_python_flake8_options = '--ignore=E501'

let g:ale_linters = {'python': ['flake8']}
let g:ale_fixers = {'python': ['isort', 'black'], 'terraform': ['terraform']}
let g:ale_terraform_fmt_executable = 'terraform'

" NERDTree
let g:NERDTreeIgnore = [
 \ '\.pyc$', 
 \ '__pycache__', 
 \ '\.db$', 
 \ '\.mypy_cache$', 
 \ '\.pytest_cache$', 
 \ '\.git$',
 \ '\.sock$',
 \ '\.pid$',
 \ '\.vim$',
 \ '\.terraform$',
 \ '\.terraform.lock.hcl$',
 \ ]
