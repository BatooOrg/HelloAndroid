package org.batoo.jpa.android;

import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Join;
import javax.persistence.criteria.JoinType;
import javax.persistence.criteria.ParameterExpression;
import javax.persistence.criteria.Root;

import org.batoo.jpa.android.model.Address;
import org.batoo.jpa.android.model.Country;
import org.batoo.jpa.android.model.Person;
import org.batoo.jpa.android.model.Phone;
import org.batoo.jpa.core.BJPASettings;
import org.batoo.jpa.core.BatooPersistenceProvider;
import org.batoo.jpa.core.JPASettings;
import org.batoo.jpa.core.jdbc.DDLMode;
import org.h2.Driver;

import android.app.Activity;
import android.os.Bundle;
import android.widget.ScrollView;
import android.widget.TextView;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class HelloAndroid extends Activity {

	private StringBuilder log;
	private long start;
	private Country country;

	@Override
	public void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		
		ScrollView sv = new ScrollView(this);
		TextView tv = new TextView(this);
		tv.setHorizontallyScrolling(true);
		tv.setVerticalScrollBarEnabled(true);
		String s = "";
		s += runTest();
		tv.setText(s);
		sv.addView(tv);
		
		setContentView(sv);
	}
	
	private void doTest() {
		
		Map<String, String> properties = Maps.newHashMap();
		properties.put(JPASettings.JDBC_DRIVER, Driver.class.getName());
		properties.put(JPASettings.JDBC_URL, "jdbc:h2:/data/data/org.batoo.jpa.android/data/hello;FILE_LOCK=FS");
		properties.put(JPASettings.JDBC_USER, "sa");
		properties.put(JPASettings.JDBC_PASSWORD, "");
		properties.put(BJPASettings.DDL, DDLMode.DROP.name());
		
		final EntityManagerFactory emf = new BatooPersistenceProvider().createEntityManagerFactory("batoo", properties , new String[]{
				Address.class.getName(), //
				Country.class.getName(), //
				Person.class.getName(), //
				Phone.class.getName()
		});

		final EntityManager em = emf.createEntityManager();
		this.country = new Country();

		this.country.setName("Turkey");
		em.getTransaction().begin();
		em.persist(this.country);
		em.getTransaction().commit();

		em.close();

		this.test(emf);
	}

	private void test(final EntityManagerFactory emf) {
		final CriteriaBuilder cb = emf.getCriteriaBuilder();
		final CriteriaQuery<Address> cq = cb.createQuery(Address.class);

		final Root<Person> r = cq.from(Person.class);
		final Join<Person, Address> a = r.join("addresses");
		a.fetch("country", JoinType.LEFT);
		a.fetch("person", JoinType.LEFT);
		cq.select(a);

		final ParameterExpression<Person> p = cb.parameter(Person.class);
		cq.where(cb.equal(r, p));

		for (int i = 0; i < 10; i++) {
			this.singleTest(emf, this.createPersons(), cq, p);
		}
	}
	
	@SuppressWarnings("unchecked")
	private List<Person>[] createPersons() {
		final List<Person>[] persons = new List[10];

		for (int i = 0; i < 10; i++) {
			persons[i] = Lists.newArrayList();

			for (int j = 0; j < 10; j++) {
				final Person person = new Person();

				person.setName("Hasan");

				final Address address = new Address();
				address.setCity("Istanbul");
				address.setPerson(person);
				address.setCountry(this.country);
				person.getAddresses().add(address);

				final Address address2 = new Address();
				address2.setCity("Istanbul");
				address2.setPerson(person);
				address2.setCountry(this.country);
				person.getAddresses().add(address2);

				final Phone phone = new Phone();
				phone.setPhoneNo("111 222-3344");
				phone.setPerson(person);
				person.getPhones().add(phone);

				final Phone phone2 = new Phone();
				phone2.setPhoneNo("111 222-3344");
				phone2.setPerson(person);
				person.getPhones().add(phone2);

				persons[i].add(person);
			}
		}

		return persons;
	}
	
	private void doPersist(final EntityManagerFactory emf, List<Person>[] persons) {
		for (final List<Person> list : persons) {
			final EntityManager em = emf.createEntityManager();

			final EntityTransaction tx = em.getTransaction();

			tx.begin();

			for (final Person person : list) {
				em.persist(person);
			}

			tx.commit();

			em.close();
		}
	}

	private void doRemove(final EntityManagerFactory emf, final List<Person>[] persons) {
		final EntityManager em = emf.createEntityManager();
		em.getTransaction().begin();

		for (int i = 0; i < 5; i++) {
			for (final Person person : persons[i]) {
				final Person person2 = em.find(Person.class, person.getId());
				em.remove(person2);
			}
		}

		em.getTransaction().commit();
		em.close();
	}
	
	private void doJpql(final EntityManagerFactory emf, final Person person) {
		for (int i = 0; i < 25; i++) {
			final EntityManager em = emf.createEntityManager();

			emf.getCriteriaBuilder();
			final TypedQuery<Address> q = em.createQuery(
				"select a from Person p inner join p.addresses a left join fetch a.country left join fetch a.person where p = :person", Address.class);

			q.setParameter("person", person);
			q.getResultList();

			em.close();
		}
	}
	
	private void doFind(final EntityManagerFactory emf, final Person person) {
		for (int i = 0; i < 250; i++) {
			final EntityManager em = emf.createEntityManager();

			final Person person2 = em.find(Person.class, person.getId());
			person2.getPhones().size();
			em.close();
		}
	}
	
	private void doUpdate(final EntityManagerFactory emf, final Person person) {
		for (int i = 0; i < 100; i++) {
			final EntityManager em = emf.createEntityManager();

			final Person person2 = em.find(Person.class, person.getId());

			final EntityTransaction tx = em.getTransaction();
			tx.begin();
			person2.setName("Ceylan" + i);
			tx.commit();

			em.close();
		}
	}

	private void doCriteria(final EntityManagerFactory emf, final Person person, CriteriaQuery<Address> cq, ParameterExpression<Person> p) {
		for (int i = 1; i < 25; i++) {
			final EntityManager em = emf.createEntityManager();

			final TypedQuery<Address> q = em.createQuery(cq);
			q.setParameter(p, person);
			q.getResultList();

			em.close();
		}
	}
	
	private void singleTest(final EntityManagerFactory emf, List<Person>[] persons, CriteriaQuery<Address> cq, ParameterExpression<Person> p) {
		this.doPersist(emf, persons);

		this.doFind(emf, persons[0].get(0));

		this.doUpdate(emf, persons[0].get(0));

		this.doCriteria(emf, persons[0].get(0), cq, p);

		this.doJpql(emf, persons[0].get(0));

		this.doRemove(emf, persons);
	}
	
	private String runTest() {
		start();
		
		try {
			doTest();
		} catch (Throwable e) {

			e.printStackTrace();
			log(e.getMessage());
		}
		
		return log.toString();
	}

	private void start() {
		log = new StringBuilder();
		
		start = System.currentTimeMillis();
	}

	private void log(String s) {
		long t = System.currentTimeMillis() - start;
		
		log.append(t).append(':').append(s).append(" \n");
	}
}