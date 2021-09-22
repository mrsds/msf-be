"""
Copyright (c) 2020 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""

import json
from msfbe.webmodel import BaseHandler, service_handler
from datetime import datetime
import subprocess
import tempfile
import os

"""

{
    "meta": {
	"version": "0.1",
	"job_submission_time": "2020-08-11T15:30:00",
	"job_owner": "jjacob",
	"job_tag": "Just a small test"
    },
    "values": {
	"lon.res": 0.05,
	"lat.res": 0.05,
	"lon.ll": -118.5,
	"lat.ll": 34.0,
	"numpix.x": 100,
	"numpix.y": 100,
	"numpar": 300,
	"nhrs": 48
    }
}
"""

@service_handler
class PleiadesRunHandlerImpl(BaseHandler):
    name = "Pleiades Run Handler"
    path = "/pleiades/run"
    description = "Enables management of Pleiades run and status APIs"
    params = {}
    singleton = True

    def __init__(self):
        BaseHandler.__init__(self)

    def run_command(self, cmd, params):
        proc_cmd = [cmd] + [str(i) for i in params]
        s = subprocess.check_call(proc_cmd)
        return s


    def run_sup(self, run_spec, to_path, username, password):
        tmp_spec = tempfile.mkstemp(prefix="pleiades_spec_", suffix=".json")



    def handle(self, computeOptions, **args):
        config = args["webconfig"]

        jobowner = computeOptions.get_argument("jobowner")

        jobtag= computeOptions.get_argument("jobtag")

        assert jobowner is not None, "jobowner cannot be null"
        assert jobtag is not None, "jobtag cannot be null"

        lonres = computeOptions.get_float_arg("lonres", None)
        latres = computeOptions.get_float_arg("latres", None)
        lonll = computeOptions.get_float_arg("lonll", None)
        latll = computeOptions.get_float_arg("latll", None)
        numpixx = computeOptions.get_int_arg("numpixx", None)
        numpixy = computeOptions.get_int_arg("numpixy", None)
        numpar = computeOptions.get_int_arg("numpar", None)
        nhrs = computeOptions.get_int_arg("nhrs", None)

        assert lonres is not None, "lonres cannot be null"
        assert latres is not None, "latres cannot be null"
        assert lonll is not None, "lonll cannot be null"
        assert latll is not None, "latll cannot be null"
        assert numpixx is not None, "numpixx cannot be null"
        assert numpixy is not None, "numpixy cannot be null"
        assert numpar is not None, "numpar cannot be null"
        assert nhrs is not None, "nhrs cannot be null"

        assert 0.0 < lonres <= 1.0, "Invalid longitude resolution."
        assert 0.0 < latres <= 1.0, "Invalid longitude resolution."
        assert -180.0 <= lonll <= 180.0 , "Longitude out of range"
        assert -90.0 <= latll <= 90.0, "Latitude out of range"
        assert numpixx > 0, "Numpixx needs to be non-zero"
        assert numpixy > 0, "Numpixy needs to be non-zero"
        assert numpar > 0, "Numpar needs to be non-zero"
        assert nhrs > 0, "nhrs needs to be non-zero"

        jobtime = datetime.utcnow()

        jobspec = {
            "meta" : {
                "version": "0.1",
                "job_submission_time": jobtime.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "job_owner": jobowner,
                "job_tag": jobtag
            },
            "values": {
                "lon.res": lonres,
                "lat.res": latres,
                "lon.ll": lonll,
                "lat.ll": latll,
                "numpix.x": numpixx,
                "numpix.y": numpixy,
                "numpar": numpar,
                "nhrs": nhrs

            }
        }

        output_dir = config.get("pleiades", "pleiades.spec.path")
        assert os.path.exists(output_dir), "Job drop directory does not exist: %s"%output_dir
        assert os.access(output_dir, os.W_OK), "Job drop directory is not writable: %s"%output_dir

        output_file = config.get("pleiades", "pleiades.spec.filename")
        output_file = jobtime.strftime(output_file)
        output_path = "%s/%s"%(output_dir, output_file)

        with open(output_path, "w") as fp:
            fp.write(json.dumps(jobspec, indent=4))

        jobspec["meta"]["jobfile"] = output_path

        class SimpleResult(object):
            def __init__(self, result):
                self.result = result

            def toJson(self):
                return json.dumps(self.result)

        return SimpleResult(jobspec)