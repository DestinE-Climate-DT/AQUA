# Customizing Git Hooks


![All-safe commits](./mrrobot_sticker.jpg)

There are two groups of git hooks: client-side and server-side. 
Client-side hooks are triggered by operations such as committing and merging.

The hooks are all stored in the `.git/hooks`. When you initialize a new repository with git init, Git populates the hooks directory with examples scripts (`pre-push.sample`, `pre-commit.sample`, etc.)

## All-Safe commits 
The pre-commit hook is run first, before you even type in a commit message.
It’s used to inspect the snapshot that’s about to be committed, to see if you’ve forgotten something, to make sure tests run, or to examine whatever you need to inspect in the code. 

1. **Copy the script to `.git/hooks` directory**
<br/> <br/>
   To enable a hook script, put a file in the hooks subdirectory of your Git directory:
   ```
   cp ./safe_commits/pre-commit .git/hooks/pre-commit
   ```

2. **Allow executing file as program** <br/>
   ```
   chmod +x .git/hooks/pre-commit
   ```


3.  **Specify the pytest.**  <br/>
   <br/>
   You can add different types of tests to the `pre-commit` script, but for our project, the most useful is to add a pytest, especially a very specific pytest, to make a committing process faster.
    <br/>
    <br/>
    In the `pre-commit` script, specify your test in line #12.
    Although you want to run all tests in the directory, add the following  line to the script: 
    <br/>
    ```
    pytest ./tests/*
    ```


4.  **Commit without tests** <br/> <br/>
   Exiting non-zero from this hook aborts the commit, although you can bypass it with git commit --no-verify.
   I.e., If you want to do a commit that fails the tests, run the following:  <br/>  <br/> 
   ``` 
   git commit --no-verify 
   ```
   <br/>



## All-Safe push 
The pre-push hook runs during git push, after the remote refs have been updated but before any objects have been transferred. A non-zero exit code will abort the push.

1. **Copy the script to `.git/hooks` directory** <br/> <br/>
   To enable a hook script, put a file in the hooks subdirectory of your Git directory: <br/> 
   ```
   cp ./safe_commits/pre-push .git/hooks/pre-push
   ```
2. **Allow executing file as program** <br/> 
   ```
   chmod +x .git/hooks/pre-push
   ```
3.  **Specify the pytest** 
    <br/> <br/> 
   In the `pre-commit` script, specify your test in line #52. 

4. **Push without tests**  <br/> <br/> 
   Exiting non-zero from this hook aborts the push, although you can bypass it with 
   ```
   git push --no-verify
   ```

<!---
## Read more 
If you are interested to read about `.git/hooks` 
[First link](https://git-scm.com/book/it/v2/Customizing-Git-Git-Hooks)
-->