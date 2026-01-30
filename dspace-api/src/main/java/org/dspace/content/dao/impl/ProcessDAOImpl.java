/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.content.dao.impl;

import static org.dspace.scripts.Process_.CREATION_TIME;

import java.sql.SQLException;

import java.text.ParseException;
import java.time.Instant;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;
import org.apache.commons.lang3.Strings;
import org.apache.logging.log4j.core.util.CronExpression;
import org.dspace.content.ProcessStatus;
import org.dspace.content.dao.ProcessDAO;
import org.dspace.core.AbstractHibernateDAO;
import org.dspace.core.Context;
import org.dspace.eperson.EPerson;
import org.dspace.scripts.Process;
import org.dspace.scripts.ProcessQueryParameterContainer;
import org.dspace.scripts.Process_;
import org.dspace.services.factory.DSpaceServicesFactory;
import org.hibernate.query.Query;

/**
 *
 * Implementation class for {@link ProcessDAO}
 */
public class ProcessDAOImpl extends AbstractHibernateDAO<Process> implements ProcessDAO {

    @Override
    public List<Process> findAllSortByScript(Context context) throws SQLException {

        CriteriaBuilder criteriaBuilder = getCriteriaBuilder(context);
        CriteriaQuery criteriaQuery = getCriteriaQuery(criteriaBuilder, Process.class);
        Root<Process> processRoot = criteriaQuery.from(Process.class);
        criteriaQuery.select(processRoot);
        criteriaQuery.orderBy(criteriaBuilder.asc(processRoot.get(Process_.name)));

        return list(context, criteriaQuery, false, Process.class, -1, -1);

    }

    @Override
    public List<Process> findAllSortByStartTime(Context context) throws SQLException {
        CriteriaBuilder criteriaBuilder = getCriteriaBuilder(context);
        CriteriaQuery criteriaQuery = getCriteriaQuery(criteriaBuilder, Process.class);
        Root<Process> processRoot = criteriaQuery.from(Process.class);
        criteriaQuery.select(processRoot);
        criteriaQuery.orderBy(criteriaBuilder.desc(processRoot.get(Process_.startTime)),
                              criteriaBuilder.desc(processRoot.get(Process_.processId)));

        return list(context, criteriaQuery, false, Process.class, -1, -1);
    }

    @Override
    public List<Process> findAll(Context context, int limit, int offset) throws SQLException {
        CriteriaBuilder criteriaBuilder = getCriteriaBuilder(context);
        CriteriaQuery criteriaQuery = getCriteriaQuery(criteriaBuilder, Process.class);
        Root<Process> processRoot = criteriaQuery.from(Process.class);
        criteriaQuery.select(processRoot);
        criteriaQuery.orderBy(criteriaBuilder.desc(processRoot.get(Process_.processId)));

        return list(context, criteriaQuery, false, Process.class, limit, offset);
    }

    @Override
    public int countRows(Context context) throws SQLException {

        CriteriaBuilder criteriaBuilder = getCriteriaBuilder(context);
        CriteriaQuery<Long> criteriaQuery = criteriaBuilder.createQuery(Long.class);
        Root<Process> processRoot = criteriaQuery.from(Process.class);
        criteriaQuery.select(criteriaBuilder.count(processRoot));

        return count(context, criteriaQuery, criteriaBuilder, processRoot);

    }

    @Override
    public List<Process> search(Context context, ProcessQueryParameterContainer processQueryParameterContainer,
                                int limit, int offset) throws SQLException {
        CriteriaBuilder criteriaBuilder = getCriteriaBuilder(context);
        CriteriaQuery criteriaQuery = getCriteriaQuery(criteriaBuilder, Process.class);
        Root<Process> processRoot = criteriaQuery.from(Process.class);
        criteriaQuery.select(processRoot);

        handleProcessQueryParameters(processQueryParameterContainer, criteriaBuilder, criteriaQuery, processRoot);
        return list(context, criteriaQuery, false, Process.class, limit, offset);

    }

    /**
     * This method will ensure that the params contained in the {@link ProcessQueryParameterContainer} are transferred
     * to the ProcessRoot and that the correct conditions apply to the query
     * @param processQueryParameterContainer    The object containing the conditions that need to be met
     * @param criteriaBuilder                   The criteriaBuilder to be used
     * @param criteriaQuery                     The criteriaQuery to be used
     * @param processRoot                       The processRoot to be used
     */
    private void handleProcessQueryParameters(ProcessQueryParameterContainer processQueryParameterContainer,
                                              CriteriaBuilder criteriaBuilder, CriteriaQuery criteriaQuery,
                                              Root<Process> processRoot) {
        addProcessQueryParameters(processQueryParameterContainer, criteriaBuilder, criteriaQuery, processRoot);
        if (Strings.CI.equals(processQueryParameterContainer.getSortOrder(), "asc")) {
            criteriaQuery
                .orderBy(criteriaBuilder.asc(processRoot.get(processQueryParameterContainer.getSortProperty())));
        } else if (Strings.CI.equals(processQueryParameterContainer.getSortOrder(), "desc")) {
            criteriaQuery
                .orderBy(criteriaBuilder.desc(processRoot.get(processQueryParameterContainer.getSortProperty())));
        }
    }

    /**
     * This method will apply the variables in the {@link ProcessQueryParameterContainer} as criteria for the
     * {@link Process} objects to the given CriteriaQuery.
     * They'll need to adhere to these variables in order to be eligible for return
     * @param processQueryParameterContainer    The object containing the variables for the {@link Process}
     *                                          to adhere to
     * @param criteriaBuilder                   The current CriteriaBuilder
     * @param criteriaQuery                     The current CriteriaQuery
     * @param processRoot                       The processRoot
     */
    private void addProcessQueryParameters(ProcessQueryParameterContainer processQueryParameterContainer,
                                           CriteriaBuilder criteriaBuilder, CriteriaQuery criteriaQuery,
                                           Root<Process> processRoot) {
        List<Predicate> andPredicates = new LinkedList<>();

        for (Map.Entry<String, Object> entry : processQueryParameterContainer.getQueryParameterMap().entrySet()) {
            andPredicates.add(criteriaBuilder.equal(processRoot.get(entry.getKey()), entry.getValue()));
        }
        criteriaQuery.where(criteriaBuilder.and(andPredicates.toArray(new Predicate[]{})));
    }

    @Override
    public int countTotalWithParameters(Context context, ProcessQueryParameterContainer processQueryParameterContainer)
        throws SQLException {

        CriteriaBuilder criteriaBuilder = getCriteriaBuilder(context);
        CriteriaQuery<Long> criteriaQuery = criteriaBuilder.createQuery(Long.class);
        Root<Process> processRoot = criteriaQuery.from(Process.class);
        criteriaQuery.select(criteriaBuilder.count(processRoot));

        addProcessQueryParameters(processQueryParameterContainer, criteriaBuilder, criteriaQuery, processRoot);
        return count(context, criteriaQuery, criteriaBuilder, processRoot);
    }


    @Override
    public List<Process> findByStatusAndCreationTimeOlderThan(Context context, List<ProcessStatus> statuses,
        Instant date) throws SQLException {

        CriteriaBuilder criteriaBuilder = getCriteriaBuilder(context);
        CriteriaQuery<Process> criteriaQuery = getCriteriaQuery(criteriaBuilder, Process.class);

        Root<Process> processRoot = criteriaQuery.from(Process.class);
        criteriaQuery.select(processRoot);

        Predicate creationTimeLessThanGivenDate = criteriaBuilder.lessThan(processRoot.get(CREATION_TIME), date);
        Predicate statusIn = processRoot.get(Process_.PROCESS_STATUS).in(statuses);
        criteriaQuery.where(criteriaBuilder.and(creationTimeLessThanGivenDate, statusIn));

        return list(context, criteriaQuery, false, Process.class, -1, -1);
    }

    @Override
    public List<Process> findRunningByInstanceIdOrExpiredHeartbeat(Context context, UUID instanceId)
        throws SQLException {

        Query<Process> query = getHibernateSession(context).createNativeQuery(
            """
               SELECT p.* FROM process AS p
               WHERE
                   (
                       p.instance_id = :instanceId
                       OR EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - p.last_heartbeat)) > 2 * :seconds
                   )
                   AND p.status IN :status
               """,
            Process.class
        );

        // Based on the Process heartbeat update cron, calculates how many seconds have passed between last execution
        // and the next. Processes with last heartbeats older than this period are considered expired.
        try {
            CronExpression cron = new CronExpression(
                DSpaceServicesFactory.getInstance().getConfigurationService().getProperty(
                    "process-heartbeat.cron",
                    "0 */1 * * * ?"
                )
            );
            final var prev = cron.getPrevFireTime(new Date());
            final var next = cron.getNextValidTimeAfter(new Date());
            final var mils = next.getTime() - prev.getTime();
            final var secs = mils / 1000;
            query.setParameter("seconds", secs);
        } catch (ParseException e) {
            // Defaults to 1min.
            query.setParameter("seconds", 60);
        }

        query.setParameter("instanceId", instanceId);
        query.setParameter("status", List.of(ProcessStatus.RUNNING.name(), ProcessStatus.SCHEDULED.name()));
        return query.getResultList();
    }

    @Override
    public void updateProcessesHeartbeat(Context context, UUID instanceId) throws SQLException {
        Query<Void> query = getHibernateSession(context).createNativeQuery(
            """
            UPDATE process AS p
            SET p.last_heartbeat = CURRENT_TIMESTAMP
            WHERE p.instance_id = :instance_id AND p.status IN :status
            """,
            Void.class
        );
        query.setParameter("instanceId", instanceId);
        query.setParameter("status", List.of(ProcessStatus.RUNNING.name(), ProcessStatus.SCHEDULED.name()));
        query.executeUpdate();
    }

    @Override
    public List<Process> findByUser(Context context, EPerson user, int limit, int offset) throws SQLException {
        CriteriaBuilder criteriaBuilder = getCriteriaBuilder(context);
        CriteriaQuery<Process> criteriaQuery = getCriteriaQuery(criteriaBuilder, Process.class);

        Root<Process> processRoot = criteriaQuery.from(Process.class);
        criteriaQuery.select(processRoot);
        criteriaQuery.where(criteriaBuilder.equal(processRoot.get(Process_.E_PERSON), user));

        List<jakarta.persistence.criteria.Order> orderList = new LinkedList<>();
        orderList.add(criteriaBuilder.desc(processRoot.get(Process_.PROCESS_ID)));
        criteriaQuery.orderBy(orderList);

        return list(context, criteriaQuery, false, Process.class, limit, offset);
    }

    @Override
    public int countByUser(Context context, EPerson user) throws SQLException {
        CriteriaBuilder criteriaBuilder = getCriteriaBuilder(context);
        CriteriaQuery<Long> criteriaQuery = criteriaBuilder.createQuery(Long.class);

        Root<Process> processRoot = criteriaQuery.from(Process.class);
        criteriaQuery.select(criteriaBuilder.count(processRoot));
        criteriaQuery.where(criteriaBuilder.equal(processRoot.get(Process_.E_PERSON), user));
        return count(context, criteriaQuery, criteriaBuilder, processRoot);
    }

}
