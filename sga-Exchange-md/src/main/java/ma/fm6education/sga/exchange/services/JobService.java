package ma.fm6education.sga.exchange.services;

import ma.fm6education.sga.exchange.config.JobsProperties;
import ma.fm6education.sga.exchange.jobs.InsertAdherentJob;
import ma.fm6education.sga.exchange.jobs.MenMesInsertJob;
import ma.fm6education.sga.exchange.jobs.TgrInsertJob;
import ma.fm6education.sga.exchange.jobs.UpdateAdherentJob;
import org.springframework.stereotype.Service;

@Service
public class JobService {

    private final JobsProperties jobsProperties;

    public JobService(JobsProperties jobsProperties) {
        this.jobsProperties = jobsProperties;
    }

    public void startMesAdherentTempJob(String fileUrl,Boolean create){
        String[] args = new String[0];
        MenMesInsertJob menMesJob = new MenMesInsertJob();
        menMesJob.fileUrl=fileUrl;
        menMesJob.create=create;
        menMesJob.url=jobsProperties.getUrl();
        menMesJob.username=jobsProperties.getUsername();
        menMesJob.password=jobsProperties.getPassword();
        menMesJob.port=jobsProperties.getPort();
        menMesJob.name=jobsProperties.getName();
        menMesJob.runJob(args);
    }

    public void startExchangeControllerJob(String fileUrl){
        String[] args = new String[0];
        TgrInsertJob menMesJob = new TgrInsertJob();
        menMesJob.fileUrl=fileUrl;
        menMesJob.url=jobsProperties.getUrl();
        menMesJob.username=jobsProperties.getUsername();
        menMesJob.password=jobsProperties.getPassword();
        menMesJob.port=jobsProperties.getPort();
        menMesJob.name=jobsProperties.getName();
        menMesJob.runJob(args);
    }

    public void addNewAdherentJob(){
        String[] args = new String[0];
        InsertAdherentJob menMesJob = new InsertAdherentJob();
        menMesJob.url=jobsProperties.getUrl();
        menMesJob.maxId= 900000;
        menMesJob.username=jobsProperties.getUsername();
        menMesJob.password=jobsProperties.getPassword();
        menMesJob.port=jobsProperties.getPort();
        menMesJob.name=jobsProperties.getName();
        menMesJob.runJob(args);
    }

    public void updateAdherentJob(){

        String[] args = new String[0];
        UpdateAdherentJob updateAdherentJob = new UpdateAdherentJob();
        updateAdherentJob.url=jobsProperties.getUrl();
        updateAdherentJob.maxId= 900000;
        updateAdherentJob.username=jobsProperties.getUsername();
        updateAdherentJob.password=jobsProperties.getPassword();
        updateAdherentJob.port=jobsProperties.getPort();
        updateAdherentJob.name=jobsProperties.getName();
        updateAdherentJob.runJob(args);
    }
}
