var fs = require("fs");
var path = require("path");
var os = require("os");
var hpc = require(path.join(__dirname, "../hpc_exec_wrapperjs/exec.js"));
var jobStatus = {
    'PD' : 'Pending',
    'R'  : 'Running'
};

// General command dictionnary keeping track of implemented features
var cmdDict = {
    "partitions"    :   ["scontrol", "show", "partition"],
    "queue"         :   ["squeue", "--format=%all", "-p"],
    "queues"        :   ["squeue", "--format=%all"],
    "nodes"         :   ["scontrol", "show", "nodes"]
};


var sActions = {
    // srun    :   "forced to run",
    // srerun  :   "successfully requeued",
    scancel    :   "successfully cancelled",
    // shold   :   "successfully put on hold",
    // srls    :   "successfully released",
};

var paramRegex = RegExp(/([^=\s]+)\=(.+?)(?:(?=\s+[a-zA-Z]+\=))/,'gm');
var slurmSep = '|';

// Helper function to return an array with [full path of exec, arguments] from a command of the cmdDict
function cmdBuilder(binPath, cmdDictElement){
    return [path.join(binPath, cmdDictElement[0])].concat(cmdDictElement.slice(1,cmdDictElement.length));
}

function getMountedPath(slurm_config, remotePath){
    return hpc.getMountedPath.apply(null, arguments);
}

function getOriginalPath(slurm_config, remotePath){
    return hpc.getOriginalPath.apply(null, arguments);
}

function createJobWorkDir(slurm_config, folder, callback){
    return hpc.createJobWorkDir.apply(null, arguments);
}

// Return the Remote Working Directory or its locally mounted Path
function getJobWorkDir(slurm_config, jobId, callback){

    // Check if the user is the owner of the job
    qstat(slurm_config, jobId, function(err,data){
        if(err){
            return callback(err);
        }
        var jobWorkingDir;
        try{
            jobWorkingDir = path.resolve(data.Variable_List.PBS_O_WORKDIR);
        }catch(e){
            return callback(new Error("Working directory not found"));
        }

        if (slurm_config.useSharedDir){
            return callback(null, getMountedPath(slurm_config, jobWorkingDir));
        }else{
            return callback(null, jobWorkingDir);
        }
    });
}

function jsonifyParams(output){
    var results = {};
    var param;
    while ((param = paramRegex.exec(output)) !== null) {
        results[param[1]]=param[2];
    }
    return results;
}

function jsonifyParsable(output){
    var results = [];

    var header = output.shift().split(slurmSep);
    for (var i = 0; i < output.length; i++) {
        if(output[i] && output[i].length>0){
            var result = {};
            var entry = output[i].split(slurmSep);
            for (var j = 0; j < entry.length; j++) {
                if(header[j] && header[j].length >0){
                    result[header[j]]=entry[j];
                }
            }
            results.push(result);
        }
    }
    return results;
}


// Generate the script to run the job and write it to the specified path
// Job Arguments taken in input : TO COMPLETE
// Return the full path of the SCRIPT
/* jobArgs = {
    shell           :   [String]      //  '/bin/bash'
    jobName         :   String      //  'XX'
    resources      :    {
        cpus-per-task    :   [String],
        tasks            :   [String],
        mem              :   [String]
      }}
    walltime        :   [String]
    stdout          :   [String]
    stderr          :   [String[
    queue           :   String
    exclusive       :   Boolean
    mail            :   String
    mailAbort       :   Boolean
    mailBegins      :   Boolean
    mailTerminates  :   Boolean
    commands        :   Array
    env             :   Object
    },
    localPath   :   'path/to/save/script'
    callback    :   callback(err,scriptFullPath)
}*/
function sscript(jobArgs, localPath, callback){
    // General SLURM command inside script
    var SLURMCommand = "#SBATCH ";
    var toWrite = "#!";
        // Job Shell: optional, default to bash
    if (jobArgs.shell !== undefined && jobArgs.shell !== ''){
        toWrite += jobArgs.shell;
    }else{
        toWrite += "/bin/bash";
    }

    var jobName = jobArgs.jobName;

    // The name has to be bash compatible: TODO expand to throw other erros
    if (jobName.search(/[^a-zA-Z0-9]/g) !== -1){
        return callback(new Error('Name cannot contain special characters'));
    }

    // Generate the script path
    var scriptFullPath = path.join(localPath,jobName);

    // Job Name
    toWrite += os.EOL + SLURMCommand + "--job-name=" + jobName;

    // Stdout: optional
    if (jobArgs.stdout !== undefined && jobArgs.stdout !== ''){
        toWrite += os.EOL + SLURMCommand + "--output=" + jobArgs.stdout;
    }
    // Stderr: optional
    if (jobArgs.stderr !== undefined && jobArgs.stderr !== ''){
        toWrite += os.EOL + SLURMCommand + "--error=" + jobArgs.stderr;
    }

    // Resources : loop on object and pass as-is
    for(var _res in jobArgs.resources){
        if (jobArgs.resources[_res] !== undefined && jobArgs.resources[_res] !== ''){
          toWrite += os.EOL + SLURMCommand + "--" + _res + "=" + jobArgs.resources[_res];
        }
    }

    // Walltime: optional
    if (jobArgs.walltime !== undefined && jobArgs.walltime !== ''){
        toWrite += os.EOL + SLURMCommand + "--time=" + jobArgs.walltime;
    }

    // Queue: none fallback to default
    if (jobArgs.queue !== undefined && jobArgs.queue !== ''){
        toWrite += os.EOL +  SLURMCommand + "--partition=" + jobArgs.queue;
    }

    // Job exclusive
    if (jobArgs.exclusive){
        toWrite += os.EOL + SLURMCommand + "--exclusive";
    }

    // EnvironmentVariables
    if(jobArgs.env){
        var jobEnv = [];
        for(var _env in jobArgs.env){
            if(_env === 'ALL'){
              jobEnv.unshift("ALL");
            }else{
              jobEnv.push(_env + '=' + jobArgs.env[_env]);
            }
        }
        toWrite += os.EOL + SLURMCommand + '--export=' + jobEnv.join(',');
    }

    // Send mail
    if (jobArgs.mail){

        toWrite += os.EOL + SLURMCommand + "--mail-user=" + jobArgs.mail;

        // Test when to send a mail
        var mailArgs = [];
        if(jobArgs.mailAbort){
          mailArgs.push("FAIL");
        }
        if(jobArgs.mailBegins){
          mailArgs.push("BEGIN");
        }
        if(jobArgs.mailTerminates){
          mailArgs.push("END");
        }

        if (mailArgs.length>0){
            toWrite += os.EOL + SLURMCommand + '--mail-type=' + jobEnv.join(',');
        }
    }

    // Write commands in plain shell including carriage returns
    toWrite += os.EOL + jobArgs.commands;

    toWrite += os.EOL;
    // Write to script, delete file if exists
    fs.unlink(scriptFullPath, function(err){
        // Ignore error if no file
        if (err && err.code !== 'ENOENT'){
            return callback(new Error("Cannot remove the existing file."));
        }
        fs.writeFile(scriptFullPath,toWrite, function(err){
            if(err){
                return callback(err);
            }

            return callback(null, {
                "message"   :   'Script for job ' + jobName + ' successfully created',
                "path"      :   scriptFullPath
            });
        });
    });
}

// Return the list of nodes
function snodes(slurm_config, controlCmd, nodeName, callback){
    // controlCmd & nodeName are optionnal so we test on the number of args
    var args = [];
    for (var i = 0; i < arguments.length; i++) {
        args.push(arguments[i]);
    }

    // first argument is the config file
    slurm_config = args.shift();

    // last argument is the callback function
    callback = args.pop();

    var remote_cmd;
    var parseOutput = true;

    // Command, Nodename or default
    if (args.length === 2){
        // Node specific info
        nodeName = args.pop();
        remote_cmd = cmdBuilder(slurm_config.binariesDir, cmdDict.nodes);
        remote_cmd.push(nodeName);
    }else{
        // Default
        remote_cmd = cmdBuilder(slurm_config.binariesDir, cmdDict.nodes);
        if(args.length === 1){
            remote_cmd.push(nodeName);
        }
    }

    var output = hpc.spawn(remote_cmd,"shell",null,slurm_config);

    // Transmit the error if any
    if (output.stderr){
        return callback(new Error(output.stderr));
    }

    if (parseOutput){
        output = output.stdout.split(os.EOL+os.EOL);

        var nodes = [];

        for (var j = 0; j < output.length-1; j++) {
            nodes.push(jsonifyParams(output[j]));
        }
        return callback(null, nodes);
    }else{
        return callback(null, {
            "message"   : 'Node ' + nodeName + ' put in ' + controlCmd + ' state.',
        });
    }
}

// Return list of partitions
function spartitions(slurm_config, partitionName, callback){
    var args = [];
    for (var i = 0; i < arguments.length; i++) {
        args.push(arguments[i]);
    }

    // first argument is the config file
    slurm_config = args.shift();

    // last argument is the callback function
    callback = args.pop();

    var remote_cmd;

    remote_cmd = cmdBuilder(slurm_config.binariesDir, cmdDict.partitions);
    // Info on a specific partition
    if (args.length == 1){
        partitionName = args.pop();
        remote_cmd.push(partitionName);
    }

    var output = hpc.spawn(remote_cmd,"shell",null,slurm_config);

    // Transmit the error if any
    if (output.stderr){
        return callback(new Error(output.stderr));
    }

    output = output.stdout.split(os.EOL+os.EOL);

    var partitions = [];

    for (var j = 0; j < output.length-1; j++) {
        partitions.push(jsonifyParams(output[j]));
    }
    return callback(null, partitions);

}

// Return list of jobs
function squeue(slurm_config, partitionName, callback){
    var args = [];
    for (var i = 0; i < arguments.length; i++) {
        args.push(arguments[i]);
    }

    // first argument is the config file
    slurm_config = args.shift();

    // last argument is the callback function
    callback = args.pop();

    var remote_cmd;

    // Info on a specific partition
    if (args.length == 1 && partitionName !== 'all'){
        partitionName = args.pop();
        remote_cmd = cmdBuilder(slurm_config.binariesDir, cmdDict.queue);
        remote_cmd.push(partitionName);
    }else{
        remote_cmd = cmdBuilder(slurm_config.binariesDir, cmdDict.queues);
    }

    var output = hpc.spawn(remote_cmd,"shell",null,slurm_config);

    // Transmit the error if any
    if (output.stderr){
        return callback(new Error(output.stderr));
    }

    var jobs  = jsonifyParsable(output.stdout.split(os.EOL));

    return callback(null, jobs);

}


// Return list of running jobs
function qstat(slurm_config, jobId, callback){
    // JobId is optionnal so we test on the number of args
    var args = [];
    // Boolean to indicate if we want the job list
    var jobList = true;

    for (var i = 0; i < arguments.length; i++) {
        args.push(arguments[i]);
    }

    // first argument is the config file
    slurm_config = args.shift();

    // last argument is the callback function
    callback = args.pop();

    var remote_cmd;

    // Info on a specific job
    if (args.length == 1){
        jobId = args.pop();
        // Qstat -f
        remote_cmd = cmdBuilder(slurm_config.binariesDir, cmdDict.job);
        // Qstat -f on all jobs
        if(jobId !== 'all'){
            // Call by short job# to avoid errors
            if(jobId.indexOf('.') > -1){
                jobId = jobId.split('.')[0];
            }
            remote_cmd.push(jobId);
            jobList = false;
        }
    }else{
        if(slurm_config.useAlternate){
            remote_cmd = cmdBuilder(slurm_config.binariesDir, cmdDict.jobsAlt);
        }else{
            remote_cmd = cmdBuilder(slurm_config.binariesDir, cmdDict.jobs);
        }
    }
    var output = hpc.spawn(remote_cmd,"shell",null,slurm_config);

    // Transmit the error if any
    if (output.stderr){
        // Treat stderr: 'Warning: Permanently added \'XXXX\' (ECDSA) to the list of known hosts.\r\n',
        return callback(new Error(output.stderr));
    }

    // If no error but zero length, the user is has no job running or is not authorized
    if (output.stdout.length === 0){
        return callback(null,[]);
    }

    if (jobList){
        var jobs = [];
        if(jobId === 'all'){
            output = output.stdout.trim().split(os.EOL+os.EOL);
            for (var m = 0; m < output.length; m++) {
                jobs.push(jsonifyQstatFull(output[m], slurm_config));
            }
        }else{
            output = output.stdout.split(os.EOL);
            // Use the alternative format
            if(slurm_config.useAlternate){
                // First 5 lines are not relevant
                for (var j = 5; j < output.length-1; j++) {
                    // First space can be truncated due to long hostnames, changing to double space
                    output[j] = output[j].replace(/^.*?\s/,function myFunction(jobname){return jobname + "  ";});

                    // Give some space to the status
                    output[j] = output[j].replace(/\s[A-Z]\s/,function myFunction(status){return "  " + status + "  ";});
                    //Split by double-space
                    output[j] = output[j].trim().split(/[\s]{2,}/);
                    jobs.push(jsonifyQstatAlt(output[j]));
                }
            }else{
                // First 2 lines are not relevant
                for (var k = 2; k < output.length-1; k++) {
                    output[k]  = output[k].trim().split(/[\s]+/);
                    jobs.push(jsonifyQstat(output[k]));
                }
            }
        }
        return callback(null, jobs);

    }else{
        return callback(null, jsonifyQstatFull(output.stdout, slurm_config));
    }
}


// Interface for sbatch
// Submit a script by its absolute path
// sbatch(
/*
        slurm_config      :   config,
        sbatchArgs        :   array of required files to send to the server with the script in 0,
        jobWorkingDir   :   working directory,
        callack(message, jobId, jobWorkingDir)
}
*/
function sbatch(slurm_config, sbatchArgs, jobWorkingDir, callback){
    var remote_cmd = cmdBuilder(slurm_config.binariesDir, cmdDict.submit);

    if(sbatchArgs.length < 1) {
        return callback(new Error('Please submit the script to run'));
    }

    // Get mounted working directory if available else return null
    var mountedPath = getMountedPath(slurm_config, jobWorkingDir);
    // Send files by the copy command defined
    for (var i = 0; i < sbatchArgs.length; i++){

        // Copy only different files
        if(!path.normalize(sbatchArgs[i]).startsWith(jobWorkingDir) && (mountedPath && !path.normalize(sbatchArgs[i]).startsWith(mountedPath))){
            var copyCmd = hpc.spawn([sbatchArgs[i],jobWorkingDir],"copy",true,slurm_config);
            if (copyCmd.stderr){
                return callback(new Error(copyCmd.stderr.replace(/\n/g,"")));
            }
        }
    }

    // Add script: first element of sbatchArgs
    var scriptName = path.basename(sbatchArgs[0]);
    remote_cmd.push(scriptName);

    // Change directory to working dir
    remote_cmd = ["cd", jobWorkingDir, "&&"].concat(remote_cmd);

    // Submit
    var output = hpc.spawn(remote_cmd,"shell",null,slurm_config);

    // Transmit the error if any
    if (output.stderr){
        return callback(new Error(output.stderr.replace(/\n/g,"")));
    }
    var jobId = output.stdout.replace(/\n/g,"");
    return callback(null, {
            "message"   : 'Job ' + jobId + ' submitted',
            "jobId"     : jobId,
            "path"      : jobWorkingDir
        });
}

// Interface to retrieve the files from a job
// Takes the jobId
/* Return {
    callack(message)
}*/

function sfind(slurm_config, jobId, callback){
    // Check if the user is the owner of the job
    getJobWorkDir(slurm_config, jobId, function(err, jobWorkingDir){
        if(err){
            return callback(err);
        }

        // Remote find command
        // TOOD: put in config file
        var remote_cmd = ["find", jobWorkingDir,"-type f", "&& find", jobWorkingDir, "-type d"];

        // List the content of the working dir
        var output = hpc.spawn(remote_cmd,"shell",slurm_config.useSharedDir,slurm_config);

        // Transmit the error if any
        if (output.stderr){
            return callback(new Error(output.stderr.replace(/\n/g,"")));
        }
        output = output.stdout.split(os.EOL);

        var fileList        = [];
        fileList.files      = [];
        fileList.folders    = [];
        var files = true;

        for (var i=0; i<output.length; i++){
            var filePath = output[i];
            if (filePath.length > 0){

                // When the cwd is returned, we have the folders
                if (path.resolve(filePath) === path.resolve(jobWorkingDir)){
                    files = false;
                }
                if (files){
                    fileList.files.push(path.resolve(output[i]));
                }else{
                    fileList.folders.push(path.resolve(output[i]));
                }
            }
        }
        return callback(null, fileList);

    });

}

// Retrieve files inside a working directory of a job from a fileList with remote or locally mounted paths
function sretrieve(slurm_config, jobId, fileList, localDir, callback){

    // Check if the user is the owner of the job
    getJobWorkDir(slurm_config, jobId, function(err, jobWorkingDir){
        if(err){
            return callback(err);
        }


        for (var file in fileList){
            var filePath = fileList[file];

            // Compare the file location with the working dir of the job
            // Filepath is already transformed to a mounted path if available
            if(path.relative(jobWorkingDir,filePath).indexOf('..') > -1){
                return callback(new Error(path.basename(filePath) + ' is not related to the job ' + jobId));
            }

            // Retrieve the file
            // TODO: treat individual error on each file
            var copyCmd = hpc.spawn([filePath,localDir],"copy",false,slurm_config);
            if (copyCmd.stderr){
                return callback(new Error(copyCmd.stderr.replace(/\n/g,"")));
            }
        }
        return callback(null,{
            "message"   : 'Files for the job ' + jobId + ' have all been retrieved in ' + localDir
        });
    });

}

// Main functions
var modules = {
    snodes              : snodes,
    squeue              : squeue,
    spartitions         : spartitions,
    sbatch              : sbatch,
    sscript             : sscript,
    sretrieve           : sretrieve,
    sfind               : sfind,
    createJobWorkDir    : createJobWorkDir,
    getJobWorkDir       : getJobWorkDir,
    getMountedPath      : getMountedPath,
    getOriginalPath     : getOriginalPath
};

/** Common interface for simple functions only taking a jobId to control a job**/
function sFn(action, msg, slurm_config, jobId, callback){
    var remote_cmd = cmdBuilder(slurm_config.binariesDir, [action]);
    remote_cmd.push(jobId);

    var output = hpc.spawn(remote_cmd,"shell",null,slurm_config);

    if (output.stderr){
        return callback(new Error(output.stderr));
    }
    // Job deleted returns
    return callback(null, {"message" : 'Job ' + jobId + ' ' + msg});
}

/** Declare simple wrapper functions from sActions
 * **/
var declareFn = function(_f){
    modules[fn] = function(){
        var args = Array.prototype.slice.call(arguments);
        args.unshift(sActions[_f]);
        args.unshift(_f);
        return sFn.apply(this, args);
    };
};

for(var fn in sActions){
    declareFn(fn);
}

// Main export
module.exports = modules;
