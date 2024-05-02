import csv
from faker import Faker
import random
import string
# Create Faker instance
fake = Faker()
f=Faker('en_IN')
# Open CSV file in write mode
with open('data.txt', mode='w', newline='') as file:
    # Create CSV writer object
    writer = csv.writer(file)
    # Write header row
    #writer.writerow(['Name', 'Age', 'Gender', 'Course', 'Roll', 'Marks', 'Email'])
    # Generate 10,000 lines of data
    for _ in range(3000):
        # Generate random data
        name = fake.first_name()
        age = random.choice([28, 29, 30])
        gender = random.choice(["Male", "Female"])
        em=random.choice(["@gmail.com","@yahoo.com","@hotmail.com","@outlook.com","@gmaii.com","@yahduu.cm","@mail.com","@look.com","@google.com"])
        n=name[0:4]
        rand=str(random.randint(100,999))
        na=f"{n}{rand}"
        email=f"{na}{em}"
        roll = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        phone = f.phone_number()
        
        #email = fake.email()
        # Write data to CSV file
        writer.writerow([f"My name is {name} and my email is {email} age {age} {gender} {phone}"])
print("sucess")




      
