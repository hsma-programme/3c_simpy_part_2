#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
from itertools import count
import random

import simpy
import pandas as pd

RESULTS_CSV = "trial_ed_results.csv"
SERVICES = ["Registration", "Triage", "ED_Assessment", "ACU_Assessment"]
PERCENTILE = 90


# Class to store global parameter values.  We don't create an instance of this
# class - we just refer to the class blueprint itself to access the numbers
# inside
class g:
    ed_inter = 8
    mean_register = 2
    mean_triage = 5
    mean_ed_assess = 30
    mean_acu_assess = 60

    prob_acu = 0.2

    number_of_receptionists = 1
    number_of_nurses = 2
    number_of_ed_doctors = 2
    number_of_acu_doctors = 1

    sim_duration = 2880
    warm_up_duration = 1440
    number_of_runs = 100


# Class representing our patients coming in to the ED.  Here, we'll store a
# patient ID and whether the patient will be sent to the ACU or stay in the
# ED, along with a method that makes that determination randomly
class ED_Patient:
    def __init__(self, p_id, prob_acu):
        self.id = p_id
        self.is_acu_patient = random.uniform(0, 1) < prob_acu
        self.queue_times = {"P_ID": p_id, "ED_Assessment": None, "ACU_Assessment": None}


# Class representing our model of the ED
class ED_Model:
    def __init__(self, run_number, csv_filename):
        self.env = simpy.Environment()
        staffing = [
            g.number_of_receptionists, g.number_of_nurses,
            g.number_of_ed_doctors, g.number_of_acu_doctors
        ]
        resources = [simpy.Resource(self.env, capacity=c) for c in staffing]
        service_times = [g.mean_register, g.mean_triage, g.mean_ed_assess, g.mean_acu_assess]
        self.resources = dict(zip(SERVICES, resources))
        self.service_times = dict(zip(SERVICES, service_times))

        self.run_number = run_number
        self.results_csv_filename = csv_filename
        self.queing_times = []

    @property
    def is_warming_up(self):
        return self.env.now > g.warm_up_duration

    # A method that generates patients arriving at the ED
    def generate_ed_arrivals(self):
        # Keep generating indefinitely whilst the simulation is running
        for patient_id in count(start=1):
            # Create a new patient
            p = ED_Patient(patient_id, g.prob_acu)
            p.queue_times["run_number"] = self.run_number
            p.queue_times["ArrivalTime"] = self.env.now

            # Get the SimPy environment to run the ed_patient_journey method
            # with this patient
            self.env.process(self.ed_patient_journey(p))

            # Randomly sample the time to the next patient arriving
            sampled_interarrival = random.expovariate(1.0 / g.ed_inter)

            # Freeze this function until that time has elapsed
            yield self.env.timeout(sampled_interarrival)

    def do_service(self, service, patient):
        """
        Generic service call:
        Waits for the service resource to be available
        Then waits for the service to complete
        The patient logs the time waiting in line
        """

        start_q_reg = self.env.now
        resource = self.resources[service]
        with resource.request() as req:
            # Wait for available staff
            yield req

            q_time = self.env.now - start_q_reg
            patient.queue_times[service] = q_time

            # Sample the duration of the service and wait until then
            mean_service_time = self.service_times[service]
            sampled_reg_duration = random.expovariate(1.0 / mean_service_time)
            yield self.env.timeout(sampled_reg_duration)

    def ed_patient_journey(self, patient):
        """Send our patient on her/his merry way..."""

        treatment = "ACU_Assessment" if patient.is_acu_patient else "ED_Assessment"
        process_services = [*SERVICES[:2], treatment]
        for service in process_services:
            yield from self.do_service(service, patient)
            patient.queue_times["ExitTime"] = self.env.now

        if not self.is_warming_up:
            self.queing_times.append(patient.queue_times)

    def store_results(self):
        """
        Store every patient's results (queuing times) for this run
        along with their patient ID to a CSV file
        """

        df = pd.DataFrame(self.queing_times)
        csv_path = Path(self.results_csv_filename)
        if csv_path.exists():
            df.to_csv(csv_path, mode="a", header=False, index=False)
        else:
            df.to_csv(csv_path, header=True, index=False)

    # The run method starts up the entity generators, and tells SimPy to start
    # running the environment for the duration specified in the g class. After
    # the simulation has run, it calls the methods that calculate run
    # results, and the method that writes these results to file
    def run(self):
        # Start entity generators
        self.env.process(self.generate_ed_arrivals())

        # Run simulation
        self.env.run(until=g.sim_duration + g.warm_up_duration)

        # Write run results to file
        self.store_results()


class Trial_Results_Calculator:
    """Class to store, calculate and manipulate trial results"""

    def __init__(self, results_csv_filename):
        self.trial_results_df = pd.read_csv(results_csv_filename)

    # A method to read in the trial results and print them for the user
    def print_trial_results(self):
        title = "TRIAL RESULTS"
        print(title)
        print("-" * len(title))

        # Take average over runs
        for service in SERVICES:
            text = f"{PERCENTILE}th percentile queue time for {service} over trial: "
            q = PERCENTILE / 100
            service_mean = self.trial_results_df[service].quantile(q)
            print(f"{text:>58}{service_mean:5.1f} minutes")



# For the number of runs specified in the g class, create an instance of the
# ED_Model class, and call its run method
def main():
    for run in range(g.number_of_runs):
        print(f"Run {run+1:03d} of {g.number_of_runs}", end="\r", flush=True)
        my_ed_model = ED_Model(run, RESULTS_CSV)
        my_ed_model.run()
    print("\n")

    # Once the trial is complete, we'll create an instance of the
    # Trial_Result_Calculator class and run the print_trial_results method
    Trial_Results_Calculator(RESULTS_CSV).print_trial_results()


main()
