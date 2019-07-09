package javautils.jdbc.hibernate;

import javautils.jdbc.PageList;
import javautils.jdbc.util.BatchSQLUtil;
import org.hibernate.*;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projection;
import org.hibernate.criterion.Projections;
import org.hibernate.jdbc.Work;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

@Component
@SuppressWarnings("unchecked")
public class HibernateSuperDao<T> {

	private static final Logger logger = LoggerFactory.getLogger(HibernateSuperDao.class);
	
	@Autowired
	private SessionFactory sessionFactory;
	
	public Session getCurrentSession() {
		return sessionFactory.getCurrentSession();
	}
	
	/**
	 * 保存
	 * @param entity
	 * @return
	 */
	@Transactional
	public boolean save(final Object entity) {
		boolean flag = false;
		try {
			getCurrentSession().save(entity);
			flag = true;
		} catch (Exception e) {
			logger.error("保存实体类异常", e);
		}
		return flag;
	}
	
	/**
	 * 更新
	 * @param entity
	 * @return
	 */
	@Transactional
	public boolean update(final Object entity) {
		boolean flag = false;
		try {
			getCurrentSession().update(entity);
			flag = true;
		} catch (Exception e) {
			logger.error("更新实体类异常", e);
		}
		return flag;
	}

	/**
	 * 更新
	 * @param hql
	 * @param values
	 * @return
	 */
	@Transactional
	public boolean update(final String hql, final Object[] values) {
		boolean flag = false;
		try {
			Query query = getCurrentSession().createQuery(hql);
			for (int i = 0; i < values.length; i++) {
				query.setParameter(String.valueOf(i), values[i]);
			}
			int result = query.executeUpdate();
			flag = result > 0 ? true : false;
		} catch (Exception e) {
			logger.error("更新实体类异常", e);
		}
		return flag;
	}
	
	/**
	 * 删除
	 * @param entity
	 * @return
	 */
	public boolean delete(final Object entity) {
		boolean flag = false;
		try {
			getCurrentSession().delete(entity);
			flag = true;
		} catch (Exception e) {
			logger.error("删除实体类异常", e);
		}
		return flag;
	}
	
	/**
	 * 删除
	 * @param hql
	 * @param values
	 * @return
	 */
	@Transactional
	public boolean delete(final String hql, final Object[] values) {
		boolean flag = false;
		try {
			Query query = getCurrentSession().createQuery(hql);
			for (int i = 0; i < values.length; i++) {
				query.setParameter(String.valueOf(i), values[i]);
			}
			int result = query.executeUpdate();
			flag = result > 0 ? true : false;
		} catch (Exception e) {
			logger.error("删除实体类异常", e);
		}
		return flag;
	}
	
	/**
	 * 查询
	 * @param hql
	 * @return
	 */
	@Transactional(readOnly = true)
	public List<T> list(final String hql) {
		try {
			Query query = getCurrentSession().createQuery(hql);
			return query.list();
		} catch (Exception e) {
			logger.error("查询出错", e);
		}
		return null;
	}
	
	/**
	 * 查询
	 * @param hql
	 * @param start
	 * @param limit
	 * @return
	 */
	@Transactional(readOnly = true)
	public List<T> list(final String hql, final int start, final int limit) {
		try {
			Query query = getCurrentSession().createQuery(hql);
			if(start >= 0 && limit > 0) {
				query.setFirstResult(start);
				query.setMaxResults(limit);
			}
			return query.list();
		} catch (Exception e) {
			logger.error("查询出错", e);
		}
		return null;
	}
	
	/**
	 * 唯一结果查询
	 * @param hql
	 * @return
	 */
	@Transactional(readOnly = true)
	public Object unique(final String hql) {
		try {
			Query query = getCurrentSession().createQuery(hql);
			return query.uniqueResult();
		} catch (Exception e) {
			logger.error("查询出错", e);
		}
		return null;
	}

	/**
	 * 唯一结果查询
	 * @param hql
	 * @param values
	 * @return
	 */
	@Transactional(readOnly = true)
	public Object unique(final String hql, final Object[] values) {
		try {
			Query query = getCurrentSession().createQuery(hql);
			for (int i = 0; i < values.length; i++) {
				query.setParameter(String.valueOf(i), values[i]);
			}
			return query.uniqueResult();
		} catch (Exception e) {
			logger.error("查询出错", e);
		}
		return null;
	}

	/**
	 * 查询
	 * @param hql
	 * @param values
	 * @return
	 */
	@Transactional(readOnly = true)
	public List<T> list(final String hql, final Object[] values) {
		try {
			Query query = getCurrentSession().createQuery(hql);
			for (int i = 0; i < values.length; i++) {
				query.setParameter(String.valueOf(i), values[i]);
			}
			return query.list();
		} catch (Exception e) {
			logger.error("查询出错", e);
		}
		return null;
	}
	
	/**
	 * 查询
	 * @param hql
	 * @param values
	 * @return
	 */
	@Transactional(readOnly = true)
	public List<?> listObject(final String hql, final Object[] values) {
		try {
			Query query = getCurrentSession().createQuery(hql);
			for (int i = 0; i < values.length; i++) {
				query.setParameter(String.valueOf(i), values[i]);
			}
			return query.list();
		} catch (Exception e) {
			logger.error("查询出错", e);
		}
		return null;
	}
	
	/**
	 * 查询
	 * @param hql
	 * @param values
	 * @param start
	 * @param limit
	 * @return
	 */
	@Transactional(readOnly = true)
	public List<T> list(final String hql, final Object[] values, final int start, final int limit) {
		try {
			Query query = getCurrentSession().createQuery(hql);
			for (int i = 0; i < values.length; i++) {
				query.setParameter(String.valueOf(i), values[i]);
			}
			if(start >= 0 && limit > 0) {
				query.setFirstResult(start);
				query.setMaxResults(limit);
			}
			return query.list();
		} catch (Exception e) {
			logger.error("查询出错", e);
		}
		return null;
	}
	
	/**
	 * SQL查询
	 * @param sql
	 * @return
	 */
	@Transactional(readOnly = true)
	public List<?> findWithSql(final String sql) {
		try {
			SQLQuery query = getCurrentSession().createSQLQuery(sql);
			return query.list();
		} catch (Exception e) {
			logger.error("查询出错", e);
		}
		return null;
	}
	
	/**
	 * SQL查询
	 * @param sql
	 * @return
	 */
	@Transactional(readOnly = true)
	public Object uniqueWithSqlCount(final String sql) {
		try {
			SQLQuery query = getCurrentSession().createSQLQuery(sql);
			return query.uniqueResult();
		} catch (Exception e) {
			logger.error("查询出错", e);
		}
		return null;
	}
	
	/**
	 * 分页查询
	 * @param querySql
	 * @param countSql
	 * @param start
	 * @param limit
	 * @return
	 */
	@Transactional(readOnly = true)
	public PageList findPageList(final String querySql, final String countSql, final int start, final int limit) {
		try {
			PageList pList = new PageList();
			// 查询总数
			SQLQuery countQuery = getCurrentSession().createSQLQuery(countSql);
			Number result = (Number) countQuery.uniqueResult();
			int count = result.intValue();
			pList.setCount(count);
			// 查询集合
			SQLQuery listQuery = getCurrentSession().createSQLQuery(querySql);
			if(start >= 0 && limit > 0) {
				listQuery.setFirstResult(start);
				listQuery.setMaxResults(limit);
			}
			List<T> list = listQuery.list();
			pList.setList(list);
			return pList;
		} catch (Exception e) {
			logger.error("查询出错", e);
		}
		return null;
	}

	/**
	 * 分页查询
	 * @param clazz
	 * @param criterions
	 * @param orders
	 * @param start
	 * @param limit
	 * @return
	 */
	@Transactional(readOnly = true)
	public PageList findPageList(final Class<T> clazz, final List<Criterion> criterions, final List<Order> orders, final int start, final int limit) {
		try {
			PageList pList = new PageList();
			Criteria criteria = getCurrentSession().createCriteria(clazz);
			for (Criterion criterion : criterions) {
				criteria.add(criterion);
			}
			for (Order order : orders) {
				criteria.addOrder(order);
			}
			// 查询总数
			criteria.setProjection(Projections.rowCount());
			Number result = (Number) criteria.uniqueResult();
			int count = result.intValue();
			pList.setCount(count);
			criteria.setProjection(null);
			// 查询集合
			if(start >= 0 && limit > 0) {
				criteria.setFirstResult(start);
				criteria.setMaxResults(limit);
			}
			List<T> list = criteria.list();
			pList.setList(list);
			return pList;
		} catch (Exception e) {
			logger.error("查询出错", e);
		}
		return null;
	}

	/**
	 * Criteria查询
	 * @param clazz
	 * @param criterions
	 * @param orders
	 * @return
	 */
	@Transactional(readOnly = true)
	public List<T> findByCriteria(final Class<T> clazz, final List<Criterion> criterions, final List<Order> orders) {
		try {
			Criteria criteria = getCurrentSession().createCriteria(clazz);
			for (Criterion criterion : criterions) {
				criteria.add(criterion);
			}
			for (Order order : orders) {
				criteria.addOrder(order);
			}
			return criteria.list();
		} catch (Exception e) {
			logger.error("查询出错", e);
		}
		return null;
	}

	/**
	 * Criteria查询
	 * @param clazz
	 * @param criterions
	 * @param orders
	 * @param limit
	 * @return
	 */
	@Transactional(readOnly = true)
	public List<T> findByCriteria(final Class<T> clazz, final List<Criterion> criterions, final List<Order> orders, final int start, final int limit) {
		try {
			Criteria criteria = getCurrentSession().createCriteria(clazz);
			for (Criterion criterion : criterions) {
				criteria.add(criterion);
			}
			for (Order order : orders) {
				criteria.addOrder(order);
			}
			if(start >= 0 && limit > 0) {
				criteria.setFirstResult(start);
				criteria.setMaxResults(limit);
			}
			return criteria.list();
		} catch (Exception e) {
			logger.error("查询出错", e);
		}
		return null;
	}
	
	/**
	 * Criteria查询
	 * @param clazz
	 * @param criterions
	 * @param orders
	 * @param projection
	 * @param start
	 * @param limit
	 * @return
	 */
	@Transactional(readOnly = true)
	public List<?> findByCriteria(final Class<T> clazz, final List<Criterion> criterions, final List<Order> orders, final Projection projection, final int start, final int limit) {
		try {
			Criteria criteria = getCurrentSession().createCriteria(clazz);
			for (Criterion criterion : criterions) {
				criteria.add(criterion);
			}
			for (Order order : orders) {
				criteria.addOrder(order);
			}
			criteria.setProjection(projection);
			if(start >= 0 && limit > 0) {
				criteria.setFirstResult(start);
				criteria.setMaxResults(limit);
			}
			return criteria.list();
		} catch (Exception e) {
			logger.error("查询出错", e);
		}
		return null;
	}
	
	/**
	 * Criteria查询
	 * @param clazz
	 * @param criterions
	 * @param orders
	 * @param projection
	 * @param start
	 * @param limit
	 * @return
	 */
	@Transactional(readOnly = true)
	public List<?> findByCriteria(final Class<T> clazz, final List<Criterion> criterions, final List<Order> orders, final Projection projection) {
		try {
			Criteria criteria = getCurrentSession().createCriteria(clazz);
			for (Criterion criterion : criterions) {
				criteria.add(criterion);
			}
			for (Order order : orders) {
				criteria.addOrder(order);
			}
			criteria.setProjection(projection);
			return criteria.list();
		} catch (Exception e) {
			logger.error("查询出错", e);
		}
		return null;
	}
	
	@Transactional
	public boolean doWork(final String sql, final List<Object[]> params) {
		boolean flag = false;
		try {
			getCurrentSession().doWork(new Work() {
				@Override
				public void execute(Connection connection) throws SQLException {
					BatchSQLUtil sqlUtil = new BatchSQLUtil(connection, sql);
					for (Object[] param : params) {
						sqlUtil.addCount(param);
					}
					sqlUtil.commit();
				}
			});
			flag = true;
		} catch (Exception e) {
			logger.error("doWork出错", e);
		}
		return flag;
	}

}