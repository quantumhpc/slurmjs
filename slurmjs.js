var hpc = require("../hpc_exec_wrapper/exec.js");
var fs = require("fs");
var path = require("path");
var os = require("os");
var jobStatus = {
    'B' : 'Begun', 
    'E' : 'Exiting', 
    'X' : 'Expired',  
    'F' : 'Finished',
    'H' : 'Held',  
    'M' : 'Moved', 
    'Q' : 'Queued', 
    'R' : 'Running', 
    'S' : 'Suspended', 
    'U' : 'UserSuspended', 
    'T' : 'Moving', 
    'W' : 'Waiting'
};

// General command dictionnary keeping track of implemented features
var cmdDict = {
    "alter"    :   ["qalter"],
    "queue"    :   ["qstat", "-Q"],
    "queues"   :   ["qstat", "-Q"],
    "job"      :   ["qstat", "-x", "-f"],
    "jobs"     :   ["qstat", "-x", "-w"],
    "jobsAlt"  :   ["qstat", "-x", "-w", "-s", "-1"],
    "node"     :   ["pbsnodes"],
    "nodes"    :   ["pbsnodes", "-a"],
    "move"     :   ["qmove"],
    "submit"   :   ["qsub"],
    "setting"  :   ["qmgr", "-c"],
    "settings" :   ["qmgr", "-c","'p s'"]
    };

var nodeControlCmd = {
    'clear'     :  ["-c"],
    'offline'   :  ["-o"],
    'reset'     :  ["-r"]
};

var qActions = {
    qrun    :   "forced to run",
    qrerun  :   "successfully requeued",
    qdel    :   "successfully cancelled",
    qhold   :   "successfully put on hold",
    qrls    :   "successfully released",
};

// Helper function to return an array with [full path of exec, arguments] from a command of the cmdDict
function cmdBuilder(binPath, cmdDictElement){
    return [path.join(binPath, cmdDictElement[0])].concat(cmdDictElement.slice(1,cmdDictElement.length));
}

function getMountedPath(pbs_config, remotePath){
    return hpc.getMountedPath.apply(null, arguments);
}

function getOriginalPath(pbs_config, remotePath){
    return hpc.getOriginalPath.apply(null, arguments);
}

function createJobWorkDir(pbs_config, folder, callback){
    return hpc.createJobWorkDir.apply(null, arguments);
}

// Return the Remote Working Directory or its locally mounted Path
function getJobWorkDir(pbs_config, jobId, callback){
    
    // Check if the user is the owner of the job
    qstat(pbs_config, jobId, function(err,data){
        if(err){
            return callback(err);
        }
        var jobWorkingDir;
        try{
            jobWorkingDir = path.resolve(data.Variable_List.PBS_O_WORKDIR);
        }catch(e){
            return callback(new Error("Working directory not found"));
        }
        
        if (pbs_config.useSharedDir){
            return callback(null, getMountedPath(pbs_config, jobWorkingDir));
        }else{
            return callback(null, jobWorkingDir);
        }
    });
}

//Takes an array to convert to JSON tree for queues and server properties
function jsonifyQmgr(output){
    var results=[];
    // JSON output will be indexed by queues and server
    results.queue=[];
    results.queues=[];
    results.server={};
    //Loop on properties
    for (var i = 0; i < output.length; i++) {
        if (output[i].indexOf('=') !== -1){
            // Split key and value to 0 and 1
            var data = output[i].split('=');
            // Split at each space to create a node in JSON
            var keys = data[0].trim().split(' ');
            var value = data[1].trim();
            //TODO: do this more effentiely
            switch (keys[1].trim()){
                case 'server':
                    results.server[keys[2].trim()] = value;
                    break;
                case 'queue':
                    // Order array under the queue name to easily store properties
                    results.queue[keys[2].trim()] = results.queue[keys[2].trim()] || {}; // initializes array if it is undefined
                    results.queue[keys[2].trim()][keys[3].trim()] = value;
                    break;
            }
        }
    }
    // Loop on the sub-array 'queue' to reorganise it more JSON-like
    for (var x in results.queue){
        // Add the name of the queue
        results.queue[x].name = x;
        results.queues.push(results.queue[x]);
    }
    // Clear the sub-array
    delete results.queue;
    
    return results;
}

function jsonifyPBSnodes(output){
    var results={};
    // Store node name
    results.name = output[0];
    // Look for properties
    for (var i = 1; i < output.length; i++) {
        if (output[i].indexOf('=')!== -1){
           // Split key and value to 0 and 1
            var data = output[i].split('=');
            results[data.shift().trim()] = data.toString().trim();
        }
    }
    // Reorganise jobs into an array with jobId & jobProcs
    if (results.jobs){
        var runningJobs = [];
        // Split by job
        var jobData = results.jobs.trim().split(/,/);
        // Parse jobs
        for (var j = 0; j < jobData.length; j++) {
            var jobProcess = jobData[j].trim().split(/\//);
            var jobId = jobProcess[0];
            var jobProcs = jobProcess[1];
            // Index by jobId to increment procs
            if(runningJobs[jobId]){
                runningJobs[jobId].jobProcs.push(jobProcs);
            }else{
                runningJobs[jobId] = {
                    jobId       :   jobId,
                    jobProcs    :   [jobProcess[1]],
                };
            }
        }
        // Reorganise into JSON
        var result = [];
        for (var job in runningJobs){
            result.push(runningJobs[job]);
        }
        results.jobs = result;
    }
    return results;
}

function jsonifyQstat(output){
    var results = {
        "jobId"     :   output[0],
        "name"      :   output[1],
        "user"      :   output[2],
        "time"      :   output[3],
        "status"    :   jobStatus[output[4]],
        "queue"     :   output[5],
    };
    return results;
}

function jsonifyQstatAlt(output){
    var results = {
        "jobId"     :   output[0],
        "user"      :   output[1],
        "queue"     :   output[2],
        "name"      :   output[3],
        "sessionID" :   output[4],
        "NDS"       :   output[5],
        "TSK"       :   output[6],
        "req_memory":   output[7],
        "req_time"  :   output[8],
        "status"    :   output[9],
        "time"      :   output[10],
        "comment"   :   output[11],
    };
    return results;
}

function jsonifyQueues(output){
    var results = {
        "name"        :   output[0],
        "maxJobs"     :   output[1],
        "totalJobs"   :   output[2],
        "enabled"     :   (output[3] === 'yes' ? true : false),
        "started"     :   (output[4] === 'yes' ? true : false),
        "queued"      :   output[5],
        "running"     :   output[6],
        "held"        :   output[7],
        "waiting"     :   output[8],
        "moving"      :   output[9],
        "exiting"     :   output[10],
        "type"        :   (output[11] === 'Exec' ? 'Execution' : 'Routing'),
        "completed"   :   output[12]
    };
    return results;
}

function jsonifyQstatFull(output, pbs_config){
    //Join truncated lines and split
    output = output.trim().replace(/\n\t/g,"").split(os.EOL);
    
    var results={};
    // First line is Job Id
    results.jobId = output[0].split(':')[1].trim();
    
    // Look for properties
    for (var i = 1; i < output.length; i++) {
        if (output[i].indexOf(' = ')!== -1){
            // Split key and value to 0 and 1
            var data = output[i].split(' = ');
            results[data[0].trim()] = data[1].trim();
        }
    }
    
    // Develop job status to be consistent
    results.job_state = jobStatus[results.job_state];
    
    // Reorganise variable list into a sub-array
    if (results.Variable_List){
        // First split by commas to separate variables
        var variables = results.Variable_List.trim().split(/[,]+/);
        // Clear
        results.Variable_List = {};
        // And split by the first = equal sign
        for (var k = 0; k < variables.length; k++) {
            // And split by the first = equal sign
            variables[k] = variables[k].split('=');
            results.Variable_List[variables[k][0]] = variables[k][1];
        }
    }
    // Transform available paths
    if (pbs_config.useSharedDir){
        results.sharedPath = {};
        if(results.Variable_List.PBS_O_WORKDIR){
            results.sharedPath.workdir      = getMountedPath(pbs_config, results.Variable_List.PBS_O_WORKDIR);
        }
        //TODO: running jobs have their stdout and stderr in the staging directory: have an option for sandbox
        var stdPath = ['Error_Path', 'Output_Path'];
        stdPath.forEach(function(_path){
            if(results[_path]){
                if(results[_path].indexOf(':') > -1){
                    results[_path] = results[_path].split(':')[1];
                }
                results.sharedPath[_path] = getMountedPath(pbs_config, results[_path]);
            }
        });
    }
    return results;
}


// Generate the script to run the job and write it to the specified path
// Job Arguments taken in input : TO COMPLETE
// Return the full path of the SCRIPT
/* jobArgs = {
    shell           :   [String]      //  '/bin/bash'
    jobName         :   String      //  'XX'
    resources      :    [{
         chunk          :   [Int],
         ncpus          :   [Int],
         mpiprocs       :   [Int],
         mem            :   [String],
         host           :   [String],
         arch           :   [String]
       },{chunk},{...}],
    custom          :   {
        custom1     :   [Int],
        custom2     :   [Int],
        custom3     :   [Int]
    }
    walltime        :   [String]      //  'walltime=01:00:00'
    workdir         :   String      //  '-d'
    stdout          :   [String]      //  '-o'
    stderr          :   [String[      //  '-e'
    queue           :   String      //  'batch'
    exclusive       :   Boolean     //  '-n'
    mail            :   String      //  'myemail@mydomain.com'
    mailAbort       :   Boolean     //  '-m a'
    mailBegins      :   Boolean     //  '-m b'
    mailTerminates  :   Boolean     //  '-m e'
    commands        :   Array       //  'main commands to run'
    env             :   Object      //  key/value pairs of environment variables
    },
    localPath   :   'path/to/save/script'
    callback    :   callback(err,scriptFullPath)
}*/
// TODO: Consider piping the commands to qsub instead of writing script
function qscript(jobArgs, localPath, callback){
    // General PBS command inside script
    var PBScommand = "#PBS ";
    var toWrite = "# Autogenerated script";
    
    var jobName = jobArgs.jobName;
    
    // The name has to be bash compatible: TODO expand to throw other erros
    if (jobName.search(/[^a-zA-Z0-9]/g) !== -1){
        return callback(new Error('Name cannot contain special characters'));
    }

    // Generate the script path
    var scriptFullPath = path.join(localPath,jobName);
    
    // Job Shell: optional, default to bash
    if (jobArgs.shell !== undefined && jobArgs.shell !== ''){
        toWrite += os.EOL + PBScommand + "-S " + jobArgs.shell;
    }else{
        toWrite += os.EOL + PBScommand + "-S /bin/bash";
    }
    // Job Name
    toWrite += os.EOL + PBScommand + "-N " + jobName;
    
    // Stdout: optional
    if (jobArgs.stdout !== undefined && jobArgs.stdout !== ''){
        toWrite += os.EOL + PBScommand + "-o " + jobArgs.stdout;
    }
    // Stderr: optional
    if (jobArgs.stderr !== undefined && jobArgs.stderr !== ''){
        toWrite += os.EOL + PBScommand + "-e " + jobArgs.stderr;
    }
    
    // Resources
    toWrite += os.EOL + PBScommand + parseResources(jobArgs.resources);
    
    // Custom Resources
    if(jobArgs.custom){
        for(var customRes in jobArgs.custom){
            toWrite += os.EOL + PBScommand + "-l " + customRes + "=" + jobArgs.custom[customRes];
        }
    }
    // Walltime: optional
    if (jobArgs.walltime !== undefined && jobArgs.walltime !== ''){
        toWrite += os.EOL + PBScommand + "-l " + jobArgs.walltime;
    }
    
    // Queue: none fallback to default
    if (jobArgs.queue !== undefined && jobArgs.queue !== ''){
        toWrite += os.EOL +  PBScommand + "-q " + jobArgs.queue;
    }
    
    // Job exclusive
    if (jobArgs.exclusive){
        toWrite += os.EOL + PBScommand + "-n";
    }
    
    
    // EnvironmentVariables
    if(jobArgs.env){
        for(var _env in jobArgs.env){
            //Quote to avoid breaks
            toWrite += os.EOL + PBScommand + '-v "' + _env + '=' + jobArgs.env[_env] + '", ';
        }
    }
    
    // Send mail
    if (jobArgs.mail){
    
        toWrite += os.EOL + PBScommand + "-M " + jobArgs.mail;
    
        // Test when to send a mail
        var mailArgs;
        if(jobArgs.mailAbort){mailArgs = '-m a';}
        if(jobArgs.mailBegins){     
          if (!mailArgs){mailArgs = '-m b';}else{mailArgs += 'b';}
        }
        if(jobArgs.mailTerminates){     
          if (!mailArgs){mailArgs = '-m e';}else{mailArgs += 'e';}
        }
        
        if (mailArgs){
            toWrite += os.EOL + PBScommand + mailArgs;
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
function qnodes(pbs_config, controlCmd, nodeName, callback){
    // controlCmd & nodeName are optionnal so we test on the number of args
    var args = [];
    for (var i = 0; i < arguments.length; i++) {
        args.push(arguments[i]);
    }

    // first argument is the config file
    pbs_config = args.shift();

    // last argument is the callback function
    callback = args.pop();
    
    var remote_cmd;
    var parseOutput = true;
    
    // Command, Nodename or default
    switch (args.length){
        case 2:
            // Node control
            nodeName = args.pop();
            controlCmd = args.pop();
            remote_cmd = cmdBuilder(pbs_config.binariesDir, cmdDict.node);
            remote_cmd = remote_cmd.concat(nodeControlCmd[controlCmd]);
            remote_cmd.push(nodeName);
            parseOutput = false;
            break;
        case 1:
            // Node specific info
            nodeName = args.pop();
            remote_cmd = cmdBuilder(pbs_config.binariesDir, cmdDict.node);
            remote_cmd.push(nodeName);
            break;
        default:
            // Default
            remote_cmd = cmdBuilder(pbs_config.binariesDir, cmdDict.nodes);
    }
    
    var output = hpc.spawn(remote_cmd,"shell",null,pbs_config);
    
    // Transmit the error if any
    if (output.stderr){
        return callback(new Error(output.stderr));
    }
    
    if (parseOutput){    
        //Detect empty values
        output = output.stdout.replace(/=,/g,"=null,");
        //Separate each node
        output = output.split(os.EOL + os.EOL);
        var nodes = [];
        //Loop on each node
        for (var j = 0; j < output.length; j++) {
            if (output[j].length>1){
                //Split at lign breaks
                output[j]  = output[j].trim().split(/[\n;]+/);
                nodes.push(jsonifyPBSnodes(output[j]));
            }
        }
        return callback(null, nodes);
    }else{
        return callback(null, { 
            "message"   : 'Node ' + nodeName + ' put in ' + controlCmd + ' state.',
        });
    }
}

// Return list of queues
function qqueues(pbs_config, queueName, callback){
    // JobId is optionnal so we test on the number of args
    var args = [];
    for (var i = 0; i < arguments.length; i++) {
        args.push(arguments[i]);
    }
    
    // first argument is the config file
    pbs_config = args.shift();

    // last argument is the callback function
    callback = args.pop();
    
    var remote_cmd;
    
    // Info on a specific job
    if (args.length == 1){
        queueName = args.pop();
        remote_cmd = cmdBuilder(pbs_config.binariesDir, cmdDict.queue);
        remote_cmd.push(queueName);
    }else{
        remote_cmd = cmdBuilder(pbs_config.binariesDir, cmdDict.queues);
    }
    
    var output = hpc.spawn(remote_cmd,"shell",null,pbs_config);

    // Transmit the error if any
    if (output.stderr){
        return callback(new Error(output.stderr));
    }
    
    output = output.stdout.split(os.EOL);
    // First 2 lines are not relevant
    var queues = [];
    for (var j = 2; j < output.length-1; j++) {
        output[j]  = output[j].trim().split(/[\s]+/);
        queues.push(jsonifyQueues(output[j]));
    }
    return callback(null, queues);
    
}
    
// Move the job to a different queue
function qmove(pbs_config, jobId, destination, callback){
    
    var remote_cmd = cmdBuilder(pbs_config.binariesDir, cmdDict.move);
    remote_cmd.push(destination);
    remote_cmd.push(jobId);
    
    var output = hpc.spawn(remote_cmd,"shell",null,pbs_config);
    
    if (output.stderr){
        return callback(new Error(output.stderr));
    }
    // Job deleted returns
    return callback(null, {"message" : 'Job ' + jobId + ' successfully moved to queue ' + destination});
}

// Return list of running jobs
function qstat(pbs_config, jobId, callback){
    // JobId is optionnal so we test on the number of args
    var args = [];
    // Boolean to indicate if we want the job list
    var jobList = true;
    
    for (var i = 0; i < arguments.length; i++) {
        args.push(arguments[i]);
    }

    // first argument is the config file
    pbs_config = args.shift();

    // last argument is the callback function
    callback = args.pop();
    
    var remote_cmd;
    
    // Info on a specific job
    if (args.length == 1){
        jobId = args.pop();
        // Qstat -f
        remote_cmd = cmdBuilder(pbs_config.binariesDir, cmdDict.job);
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
        if(pbs_config.useAlternate){
            remote_cmd = cmdBuilder(pbs_config.binariesDir, cmdDict.jobsAlt);
        }else{
            remote_cmd = cmdBuilder(pbs_config.binariesDir, cmdDict.jobs);
        }
    }
    var output = hpc.spawn(remote_cmd,"shell",null,pbs_config);
    
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
                jobs.push(jsonifyQstatFull(output[m], pbs_config));
            }
        }else{
            output = output.stdout.split(os.EOL);
            // Use the alternative format
            if(pbs_config.useAlternate){
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
        return callback(null, jsonifyQstatFull(output.stdout, pbs_config));
    }
}

// Interface for qmgr
// For now only display server info
function qmgr(pbs_config, qmgrCmd, callback){
    // qmgrCmd is optionnal so we test on the number of args
    var args = [];
    for (var i = 0; i < arguments.length; i++) {
        args.push(arguments[i]);
    }

    // first argument is the config file
    pbs_config = args.shift();

    // last argument is the callback function
    callback = args.pop();
    
    var remote_cmd = pbs_config.binariesDir;
    if (args.length === 0){
        // Default print everything
        remote_cmd = cmdBuilder(pbs_config.binariesDir, cmdDict.settings);
    }else{
        // TODO : handles complex qmgr commands
        remote_cmd = cmdBuilder(pbs_config.binariesDir, cmdDict.setting);
        remote_cmd.push(args.pop());
        return callback(new Error('not yet implemented'));
    }
    var output = hpc.spawn(remote_cmd,"shell",null,pbs_config);
    
    // Transmit the error if any
    if (output.stderr){
        return callback(new Error(output.stderr));
    }
    
    output = output.stdout.split(os.EOL);
    var qmgrInfo = jsonifyQmgr(output);
    
    return callback(null, qmgrInfo);
}


// Interface for qsub
// Submit a script by its absolute path
// qsub(
/*    
        pbs_config      :   config,
        qsubArgs        :   array of required files to send to the server with the script in 0,
        jobWorkingDir   :   working directory,
        callack(message, jobId, jobWorkingDir)
}
*/
function qsub(pbs_config, qsubArgs, jobWorkingDir, callback){
    var remote_cmd = cmdBuilder(pbs_config.binariesDir, cmdDict.submit);
    
    if(qsubArgs.length < 1) {
        return callback(new Error('Please submit the script to run'));  
    }
    
    // Get mounted working directory if available else return null
    var mountedPath = getMountedPath(pbs_config, jobWorkingDir);
    // Send files by the copy command defined
    for (var i = 0; i < qsubArgs.length; i++){
        
        // Copy only different files
        // if(path.normalize(qsubArgs[i]) !== path.join(jobWorkingDir, path.basename(qsubArgs[i]))){
        if(!path.normalize(qsubArgs[i]).startsWith(jobWorkingDir) && (mountedPath && !path.normalize(qsubArgs[i]).startsWith(mountedPath))){
            var copyCmd = hpc.spawn([qsubArgs[i],jobWorkingDir],"copy",true,pbs_config);
            if (copyCmd.stderr){
                return callback(new Error(copyCmd.stderr.replace(/\n/g,"")));
            }
        }
    }
    
    // Add script: first element of qsubArgs
    var scriptName = path.basename(qsubArgs[0]);
    remote_cmd.push(scriptName);
    
    // Change directory to working dir
    remote_cmd = ["cd", jobWorkingDir, "&&"].concat(remote_cmd);
    
    // Submit
    var output = hpc.spawn(remote_cmd,"shell",null,pbs_config);
    
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

// Interface for qalter
// Modify job parameters inline
// qalter(
/*    
        pbs_config      :   config,
        jobId           :   jobId,
        jobSettings     :  {
            jobName     :   string
            priority    :   -1024 < n < 1023
        },
        callack(err, success)
}
*/
function qalter(pbs_config, jobId, jobSettings, callback){
    var remote_cmd = cmdBuilder(pbs_config.binariesDir, cmdDict.alter);
    
    // Job name
    if(jobSettings.jobName){
        remote_cmd.push('-N');
        remote_cmd.push(jobSettings.jobName);
    }
    // Priority
    if(jobSettings.priority){
        // Parse number
        jobSettings.priority = Math.max(Math.min(Number(jobSettings.priority), 1023), -1024);
        remote_cmd.push('-p');
        remote_cmd.push(jobSettings.priority);
    }
    remote_cmd.push(jobId);
    
    // Submit
    var output = hpc.spawn(remote_cmd,"shell",null,pbs_config);
    
    // Transmit the error if any
    if (output.stderr){
        return callback(new Error(output.stderr.replace(/\n/g,"")));
    }
    
    return callback(null, 'Job ' + jobId + ' successfully altered');
}

// Interface to retrieve the files from a job
// Takes the jobId
/* Return {
    callack(message)
}*/

function qfind(pbs_config, jobId, callback){
    // Check if the user is the owner of the job
    getJobWorkDir(pbs_config, jobId, function(err, jobWorkingDir){
        if(err){
            return callback(err);
        }
        
        // Remote find command
        // TOOD: put in config file
        var remote_cmd = ["find", jobWorkingDir,"-type f", "&& find", jobWorkingDir, "-type d"];
        
        // List the content of the working dir
        var output = hpc.spawn(remote_cmd,"shell",pbs_config.useSharedDir,pbs_config);
        
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
function qretrieve(pbs_config, jobId, fileList, localDir, callback){
    
    // Check if the user is the owner of the job
    getJobWorkDir(pbs_config, jobId, function(err, jobWorkingDir){
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
            var copyCmd = hpc.spawn([filePath,localDir],"copy",false,pbs_config);
            if (copyCmd.stderr){
                return callback(new Error(copyCmd.stderr.replace(/\n/g,"")));
            }
        }
        return callback(null,{
            "message"   : 'Files for the job ' + jobId + ' have all been retrieved in ' + localDir
        });
    });

}

// Parse resources and return the qsub -l  statement
//TODO: check against resources_available
/**
 * {
     chunk          :   [Int],
     ncpus          :   [Int],
     mpiprocs       :   [Int],
     mem            :   [String],
     host           :   [String],
     arch           :   [String]
   }
**/
function parseResources(resources){
    // Resources
    var select = "-l select=";
    
    if(!(Array.isArray(resources))){
        resources = [resources];
    }
    
    for(var statement in resources){
        if(statement>0){
            select+="+";
        }
        // Chunk: default to 1
        if (resources[statement].chunk !== undefined && resources[statement].chunk !== ''){
            select+=resources[statement].chunk;
        }else{
            select+="1";
        }
        
        // Loop on the rest
        for(var res in resources[statement]){
            if (res !== "chunk" && resources[statement][res] !== undefined && resources[statement][res] !== ''){
                select+=":" + res + "=" + resources[statement][res];
            }
        }
    }
    return select;
}

// Main functions
var modules = {
    qnodes              : qnodes,
    qstat               : qstat,
    qqueues             : qqueues,
    qmove               : qmove,
    qmgr                : qmgr,
    qsub                : qsub,
    qalter              : qalter,
    qscript             : qscript,
    qretrieve           : qretrieve,
    qfind               : qfind,
    createJobWorkDir    : createJobWorkDir,
    getJobWorkDir       : getJobWorkDir,
    getMountedPath      : getMountedPath,
    getOriginalPath     : getOriginalPath
};

/** Common interface for simple functions only taking a jobId to control a job**/
function qFn(action, msg, pbs_config, jobId, callback){
    var remote_cmd = cmdBuilder(pbs_config.binariesDir, [action]);
    remote_cmd.push(jobId);
    
    var output = hpc.spawn(remote_cmd,"shell",null,pbs_config);
    
    if (output.stderr){
        return callback(new Error(output.stderr));
    }
    // Job deleted returns
    return callback(null, {"message" : 'Job ' + jobId + ' ' + msg});
}

/** Declare simple wrapper functions
 * qdel     :       Delete the specified job Id and return the message and the status code
 * qhold   :
 * qrls     :
 * **/
var declareFn = function(_f){
    modules[fn] = function(){
        var args = Array.prototype.slice.call(arguments);
        args.unshift(qActions[_f]);
        args.unshift(_f);
        return qFn.apply(this, args);
    };
};

for(var fn in qActions){
    declareFn(fn);
}

// Main export
module.exports = modules;