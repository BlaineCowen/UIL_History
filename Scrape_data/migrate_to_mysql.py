from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound


Base = declarative_base()


class Pml(Base):
    __tablename__ = "pml"
    code = Column(String(255), primary_key=True)
    event_name = Column(String(255))
    title = Column(String(255))
    composer = Column(String(255), nullable=True)
    arranger = Column(String(255), nullable=True)
    publisher = Column(Text, nullable=True)
    grade = Column(String(255))
    specification = Column(Text, nullable=True)


class Results(Base):
    __tablename__ = "results"
    contest_date = Column(Date)
    event = Column(String(255))
    region = Column(String(255))
    school = Column(String(255))
    tea_code = Column(String(255), nullable=True)
    city = Column(String(255), nullable=True)
    director = Column(String(255))
    additional_director = Column(String(255), nullable=True)
    accompanist = Column(String(255), nullable=True)
    conference = Column(String(255), nullable=True)
    classification = Column(String(255), nullable=True)
    entry_number = Column(String(255), primary_key=True)
    title_1 = Column(String(255), nullable=True)
    composer_1 = Column(String(255), nullable=True)
    title_2 = Column(String(255), nullable=True)
    composer_2 = Column(String(255), nullable=True)
    title_3 = Column(String(255), nullable=True)
    composer_3 = Column(String(255), nullable=True)
    concert_judge = Column(String(255), nullable=True)
    concert_judge_1 = Column(String(255), nullable=True)
    concert_judge_2 = Column(String(255), nullable=True)
    concert_score_1 = Column(Integer, nullable=True)
    concert_score_2 = Column(Integer, nullable=True)
    concert_score_3 = Column(Integer, nullable=True)
    concert_final_score = Column(Integer, nullable=True)
    sight_reading_judge = Column(String(255), nullable=True)
    sight_reading_judge_1 = Column(String(255), nullable=True)
    sight_reading_judge_2 = Column(String(255), nullable=True)
    sight_reading_score_1 = Column(Integer, nullable=True)
    sight_reading_score_2 = Column(Integer, nullable=True)
    sight_reading_score_3 = Column(Integer, nullable=True)
    sight_reading_final_score = Column(Integer, nullable=True)
    award = Column(String(255), nullable=True)
    code_1 = Column(String(255), ForeignKey("pml.code"), nullable=True)
    code_2 = Column(String(255), ForeignKey("pml.code"), nullable=True)
    code_3 = Column(String(255), ForeignKey("pml.code"), nullable=True)


sqlite_engine = create_engine("sqlite:///uil.db")
mysql_engine = create_engine(
    "mysql+pymysql://root:xudfoz-zipba4-gychIm@localhost:3306/uil"
)

SQLiteSession = sessionmaker(bind=sqlite_engine)
MySQLSession = sessionmaker(bind=mysql_engine)

sqlite_session = SQLiteSession()
mysql_session = MySQLSession()

# drop any existing tables first
Base.metadata.drop_all(mysql_engine)

Base.metadata.create_all(mysql_engine)

for table in [Pml, Results]:
    records = sqlite_session.query(table).all()
    for record in records:
        # check if table is results
        if table == Results:
            # check if code_1, code_2, code_3 are None
            if record.code_1 == "None":
                record.code_1 = None
            if record.code_2 == "None":
                record.code_2 = None
            if record.code_3 == "None":
                record.code_3 = None
        mysql_session.merge(record)

mysql_session.commit()
mysql_session.close()
sqlite_session.close()
