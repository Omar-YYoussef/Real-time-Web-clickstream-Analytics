import matplotlib.pyplot as plt

# Load processed data or results
# For example:
page_visit_counts = [('/home', 100), ('/products', 80), ('/about', 60)]

# Plotting
pages, counts = zip(*page_visit_counts)
plt.bar(pages, counts)
plt.xlabel('Page URL')
plt.ylabel('Visit Count')
plt.title('Page Visit Counts')
plt.xticks(rotation=45)
plt.tight_layout()

# Save or display plot
plt.savefig('page_visit_counts.png')
plt.show()
