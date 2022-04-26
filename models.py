from itertools import accumulate
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import JSON, Column, ForeignKey, Integer, String, Date, DECIMAL
from functools import wraps
from typing import Union
Base = declarative_base()


def auto_init(exclude: Union[set, list] = None):  # sourcery no-metrics
    """Wraps the `__init__` method of a class to automatically set the common
    attributes.

    Args:
        exclude (Union[set, list], optional): [description]. Defaults to None.
    """

    exclude = exclude or set()
    # exclude.add("id")

    def decorator(init):
        @wraps(init)
        def wrapper(self, *args, **kwargs):  # sourcery no-metrics
            """
            Custom initializer that allows nested children initialization.
            Only keys that are present as instance's class attributes are allowed.
            These could be, for example, any mapped columns or relationships.

            Code inspired from GitHub.
            Ref: https://github.com/tiangolo/fastapi/issues/2194
            """
            cls = self.__class__
            model_columns = self.__mapper__.columns
            # relationships = self.__mapper__.relationships

            for key, val in kwargs.items():
                if key in exclude:
                    continue

                if not hasattr(cls, key):
                    continue
                    # raise TypeError(f"Invalid keyword argument: {key}")

                if key in model_columns:
                    setattr(self, key, val)
                    continue

            return init(self, *args, **kwargs)

        return wrapper

    return decorator


class DBModel(Base):
    @auto_init(exclude={})
    def __init__(self, **_):
        pass

class User(DBModel):
    __tablename__ = 'users'

    sync_id = Column(Integer, primary_key=True)
    id = Column(Integer)
    email = Column(String)
    name = Column(String)
    phone = Column(String)
    promote_activated = Column(Date)

    ###### self declare ######
    school = Column(String)
    school_region = Column(String)
    grade = Column(String)
    career = Column(String)
    industry = Column(String)
    banbie = Column(String)
    ###########################

    remark = Column(String)
    created_at = Column(Date)
    updated_at = Column(Date)

    ### relationship ###


class Paper(Base):
    __tablename__ = 'papers'

    sync_id = Column(Integer, primary_key=True)
    id = Column(Integer)
    description = Column(String)

    created_at = Column(Date)
    updated_at = Column(Date)
    deleted_at = Column(Date)

    


class Question(Base):
    __tablename__ = 'questions'

    sync_id = Column(Integer, primary_key=True)
    id = Column(Integer)
    score = Column(Integer)

    kind = Column(Integer)
    paper_pageable_type = Column(Integer)

    paper_id = Column(Integer, ForeignKey('papers.id'))

    created_at = Column(Date)
    updated_at = Column(Date)
    deleted_at = Column(Date)


class Tag(Base):
    __tablename__ = 'tags'

    sync_id = Column(Integer, primary_key=True)
    id = Column(Integer)
    name = Column(String)
    tagging_count = Column(Integer)


class Tagging(Base):
    __tablename__ = 'taggings'

    sync_id = Column(Integer, primary_key=True)
    id = Column(Integer)
    tag_id = Column(Integer)
    taggable_type = Column(String)
    taggable_id = Column(Integer)

    created_at = Column(Date)


class Answer(Base):
    __tablename__ = 'answers'

    writing = Column(String)

    created_at = Column(Date)
    updated_at = Column(Date)
    deleted_at = Column(Date)


class UsersQuestion(Base):
    __tablename__ = 'users_questions'

    sync_id = Column(Integer, primary_key=True)
    id = Column(Integer)
    score = Column(Integer)

    kind = Column(Integer)
    paper_pageable_type = Column(Integer)

    user_id = Column(Integer, ForeignKey('users.id'))
    question_id = Column(Integer, ForeignKey('questions.id'))
    users_paper_id = Column(Integer, ForeignKey('users_papers.id'))
    answer_id = Column(Integer, ForeignKey('answers.id'))
    correction_id = Column(Integer, ForeignKey('answers.id'))

    created_at = Column(Date)
    updated_at = Column(Date)
    deleted_at = Column(Date)


class UsersPaper(Base):
    __tablename__ = 'users_papers'

    sync_id = Column(Integer, primary_key=True)
    id = Column(Integer)

    score = Column(DECIMAL)
    accumulate_score = Column(DECIMAL)
    level = Column(String)

    status = Column(Integer)

    meta = Column(JSON)
    submited_at = Column(Date)
    corrected_at = Column(Date)

    answered_count = Column(Integer)

    teacher_id = Column(Integer)
    audit_teacher_id = Column(Integer)

    # relationship #
    user_id = Column(Integer, ForeignKey('users.id'))
    paper_id = Column(Integer, ForeignKey('papers.id'))

    browser = Column(String)
    created_at = Column(Date)
    updated_at = Column(Date)
    deleted_at = Column(Date)


class School(Base):
    __tablename__ = 'schools'

    sync_id = Column(Integer, primary_key=True)
    id = Column(Integer)
    name = Column(String)
    region = Column(String)

    created_at = Column(Date)
    updated_at = Column(Date)


class SchoolUser(Base):
    __tablename__ = 'school_users'

    sync_id = Column(Integer, primary_key=True)
    id = Column(Integer)
    school_id = Column(Integer, ForeignKey('schools.id'))
    user_id = Column(Integer, ForeignKey('users.id'))

    role = Column(Integer)
    status = Column(Integer)
    stage = Column(Integer)

    created_at = Column(Date)
    updated_at = Column(Date)
