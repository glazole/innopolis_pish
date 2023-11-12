# Создаем класс
class Dog():
    """Простая модель собаки"""
    def __init__(self, name, age):
        """Инициализируются атрибуты name и age"""
        self.name = name
        self.age = age

# Функция "сидеть"
    def sit(self):
        """Собака садится по команде"""
        print(f"{self.name} is now sitting")

# Функция "перекатываться"
    def roll_over(self):
        """Собака перекатывается по команде"""
        print(f"{self.name} rolled over!")

# Создаем экземпляр класса Dog()
my_dog = Dog('Willie', 6)

# Обращаемся к атрибутам экземпляра класса
print(f"My dog's name is {my_dog.name}")
print(f"My dog is {my_dog.age} years old")

# Вызываем методы экземпляра класса - функции собаки - сидеть, перекатываться
# Для вызова метода нужно указать экземпляр класса, my_dog в данном случае, и вызываемый метод

my_dog.sit()
my_dog.roll_over()

# Создаем очередной экземпляр класса Dog()
your_dog = Dog('Luci', 3)
print(f"\nMy dog's name is {my_dog.name}")
print(f"My dog is {my_dog.age} years old")
my_dog.sit()

print(f"\nYour dog's name is {your_dog.name}")
print(f"Your dog is {your_dog.age} years old")
your_dog.sit()