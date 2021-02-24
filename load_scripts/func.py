import datetime, sys, re, os
from contextlib import contextmanager
from subprocess import call
from db import Session, Base


@contextmanager
def session_scope(logger=None, mode="update"):
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()

        if logger != None and mode == "upload":
            logger.info("Pro was succefully uploaded")
        if logger != None and mode == "update":
            logger.info("Pro was succefully updated")

    except Exception as exp:

        session.rollback()
        if logger != None:
            logger.warning("there was a problem updating this pro")
            logger.warning(exp)
    finally:
        session.close()


def merge_pro(log_message, columns, dlicates, p_ind, keep_id, sql_ids):
    Company = Base.classes["companies_company"]

    for col in columns:
        if dlicates.loc[p_ind[0], col] != dlicates.loc[p_ind[1], col]:
            print(f"Choose for {col}")
            print(f"0) {dlicates.loc[p_ind[0], col]}")
            print(f"1) {dlicates.loc[p_ind[1], col]}")
            if len(p_ind) == 3:
                print(f" 2) {dlicates.loc[p_ind[2], col]}")
            elif len(p_ind) > 3:
                print(f"3) {dlicates.loc[p_ind[3], col]}")

            choice = input("choice:  ")

            if choice == 's' or col == 'yelp_url':
                return 'skip'

            new_value = dlicates.loc[p_ind[int(choice)], col]

            # confirm the value
            confirm = input(f"Confirm Value: {new_value}\n Enter 'n' to change answer.")
            if confirm == "n":
                choice = input("new_choice: ")
                new_value = dlicates.loc[p_ind[int(choice)], col]
            else:
                pass

            # log  logo so in case it need to be deleted specifically
            if col == "logo":
                log_message = (
                    log_message
                    + f"\t\tduplicates logos: {list(dlicates['logo'].drop(p_ind[int(choice)]))}\n"
                )

            with session_scope() as session:
                keep_pro = session.query(Company).filter(Company.id == keep_id).one()
                log_message = log_message + f"\tModifying column: {col}\n"
                log_message = (
                    log_message + f"\t\t{to_dict(keep_pro)[col]} => {new_value}\n"
                )

                ### update the column in the pro
                session.query(Company).filter(Company.id == keep_id).update(
                    {col: new_value}, synchronize_session="fetch"
                )

    return log_message


def get_slugz():
    Company = Base.classes["companies_company"]
    Distributor = Base.classes["companies_distributor"]
    Manufacturer = Base.classes["companies_manufacturer"]
    Partner = Base.classes["partners_partner"]

    with session_scope() as session:
        comp_slugs = [x[0] for x in session.query(Company.slug).all()]
        distr_slugs = [x[0] for x in session.query(Distributor.slug).all()]
        manu_slugs = [x[0] for x in session.query(Manufacturer.slug).all()]
        partner_slugs = [x[0] for x in session.query(Partner.slug).all()]

    return comp_slugs + distr_slugs + manu_slugs + partner_slugs


def reduce30(o_slug):
    lastchar_index = o_slug.rindex(re.findall(r"(?:.(?![A-z]))+$", o_slug)[0])

    o_slug = list(o_slug)

    del o_slug[lastchar_index]

    return "".join(o_slug)


def unique_slug(slug):
    slugz = get_slugz()

    if slug in slugz:
        if len(slug) < 30:
            if re.search(r"\d+$", slug) != None:
                input_num = int(re.findall(r"\d+$", slug)[0]) + 1
                add1_slug = slug.replace(re.findall(r"\d+$", slug)[0], str(input_num))

                while len(add1_slug) > 30:
                    add1_slug = reduce30(add1_slug)

                if add1_slug in slugz:
                    return unique_slug(add1_slug)
                else:
                    good_slug = add1_slug

            elif re.search(r"\d+$", slug) == None:
                if len(slug) < 28:
                    return unique_slug(f"{slug}-1")
                else:
                    return unique_slug(f"{slug[:28]}-1")

        elif len(slug) == 30:
            if re.search(r"\d+$", slug) != None:
                input_num = int(re.findall(r"\d+$", slug)[0]) + 1
                add1_slug = slug.replace(re.findall(r"\d+$", slug)[0], str(input_num))

                while len(add1_slug) > 30:
                    add1_slug = reduce30(add1_slug)

                if add1_slug in slugz:
                    return unique_slug(add1_slug)
                elif add1_slug not in slugz:
                    good_slug = add1_slug

            else:
                return unique_slug(f"{slug[:28]}-1")

        return good_slug

    elif slug not in slugz:

        return slug


def slug_logic(name):
    clean_str = name.translate(
        {ord(c): "" for c in r"[!”#$%&’()*+,./:;<=>?@[\]^`{|}~]"}
    )
    spaces2hyphens = [str(s).translate({ord(" "): ord("-")}) for s in clean_str]
    alpha_name = "".join([s.lower() for s in spaces2hyphens])
    final_slug = unique_slug(alpha_name[:30])

    return final_slug


clean_phone = lambda phone: phone.translate({ord(c): "" for c in "[()[\]- ]"})


def bbb_rating_mapping(rating):
    if rating == "A+":
        return 4.2
    elif rating == "A":
        return 4
    elif rating == "A-":
        return 3.7
    elif rating == "B+":
        return 3.2
    elif rating == "B":
        return 3
    elif rating == "B-":
        return 2.7
    elif rating == "C+":
        return 2.2
    elif rating == "C":
        return 2
    elif rating == "C-":
        return 1.7
    elif rating == "D+":
        return 1.2
    elif rating == "D":
        return 1
    elif rating == "D-":
        return 0.7
    elif rating == "F":
        return 0


def industry_match(pi):
    if pi == "ac  heating":
        rid = 1
    elif pi == "appliance":
        rid = 2
    elif "cleaning services":
        rid = 3
    elif pi == "plumbing":
        rid = 5
    elif pi == "handyman":
        rid = 6
    elif pi == "garage  gate":
        rid = 7
    elif pi == "pest  termite":
        rid = 8
    elif pi == "locksmith":
        rid = 12
    elif pi == "lawn care":
        rid = 13
    elif pi == "pool and spa":
        rid = 14
    elif pi == "movers":
        rid = 19
    elif pi == "construction  remodel":
        rid = 20
    elif pi == "electrical":
        rid = 21
    elif pi == "painting":
        rid = 23

    return rid


clean_special = lambda x: x.translate(
    {ord(c): "" for c in "[!”#$%&’()*+,./:;<=>?@[\]^`{|}~]"}
)


def clean_str(string):
    """
    Tokenization/string cleaning for dataset
    Every dataset is lower cased except
    """
    string = re.sub(r"[^\x00-\x7f]", " ", string)  # remove non ascii characters
    string = re.sub(r"http\S+", "", string)  # remove URLs
    string = re.sub(r"\n", " ", string)
    string = re.sub(r"\r", " ", string)
    string = re.sub(r"[0-9]", " ", string)
    string = re.sub(r"\'", " ", string)
    string = re.sub(r"\"", " ", string)
    string = re.sub(r"", "", string)

    return string.strip().lower()


def to_dict(self):
    return {
        column.name: getattr(self, column.name)
        if not isinstance(getattr(self, column.name), datetime.datetime)
        else getattr(self, column.name).isoformat()
        for column in self.__table__.columns
    }


def clear():
    # check and make call for specific operating system
    _ = call("clear" if os.name == "posix" else "cls")
